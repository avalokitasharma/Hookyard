package webhook

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/avalokitasharma/HookYard/delivery-service/internal/attempt"
	"github.com/avalokitasharma/HookYard/delivery-service/internal/delivery"
)

const (
	defaultRequestTimeout = 10 * time.Second
	maxResponseBody       = 64 * 1024
)

type Client struct {
	httpClient *http.Client
}

func NewClient() *Client {
	transport := &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &Client{
		httpClient: &http.Client{
			Transport: transport,
		},
	}
}

// post performs exactly one HTTP POST request.
// It contains no retry
func (c *Client) post(ctx context.Context, url string, body []byte, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		url,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	return c.httpClient.Do(req)
}

func (c *Client) Send(ctx context.Context, d *delivery.Delivery, body []byte) attempt.Result {
	start := time.Now()

	timeout := time.Duration(d.EndpointSnapshot.RequestTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}

	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	secret := d.EndpointSnapshot.SecretEncrypted

	headers := map[string]string{
		"Content-Type":          "application/json",
		"User-Agent":            "Hookyard-Delivery/1.0",
		"X-Hookyard-Event-ID":   d.EventID.String(),
		"X-Hookyard-Event-Type": d.EndpointSnapshot.EventType,
		"X-Hookyard-Timestamp":  timestamp,
		"X-Hookyard-Signature":  Sign(secret, timestamp, body),
	}

	resp, err := c.post(
		requestCtx,
		d.EndpointSnapshot.URL,
		body,
		headers,
	)
	if err != nil {
		code := "HTTP_ERROR"

		if requestCtx.Err() == context.DeadlineExceeded {
			code = "TIMEOUT"
		}

		msg := err.Error()

		return attempt.Result{
			Status:       attempt.StatusRetryable,
			ErrorCode:    &code,
			ErrorMessage: &msg,
			Latency:      time.Since(start),
		}
	}

	defer resp.Body.Close()

	responseBytes, err := readResponseBody(resp.Body)
	if err != nil {
		code := "RESPONSE_READ_ERROR"
		msg := err.Error()

		return attempt.Result{
			Status:       attempt.StatusRetryable,
			ErrorCode:    &code,
			ErrorMessage: &msg,
			Latency:      time.Since(start),
		}
	}

	statusCode := resp.StatusCode

	result := attempt.Result{
		HTTPStatusCode:  &statusCode,
		ResponseHeaders: responseHeaders(resp),
		ResponseBody:    string(responseBytes),
		Latency:         time.Since(start),
	}

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		result.Status = attempt.StatusSuccess

	case resp.StatusCode == http.StatusRequestTimeout ||
		resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode >= 500:

		result.Status = attempt.StatusRetryable

		code := fmt.Sprintf("HTTP_%d", resp.StatusCode)
		result.ErrorCode = &code

		msg := fmt.Sprintf(
			"consumer returned HTTP %d",
			resp.StatusCode,
		)
		result.ErrorMessage = &msg

	default:
		result.Status = attempt.StatusPermanent

		code := fmt.Sprintf("HTTP_%d", resp.StatusCode)
		result.ErrorCode = &code

		msg := fmt.Sprintf(
			"consumer returned HTTP %d",
			resp.StatusCode,
		)
		result.ErrorMessage = &msg
	}

	return result
}

func readResponseBody(body io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(body, maxResponseBody))
}

func responseHeaders(resp *http.Response) map[string]string {
	headers := make(map[string]string, len(resp.Header))

	for key, values := range resp.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	return headers
}
