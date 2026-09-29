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
)

type Request struct {
	URL              string
	Secret           []byte
	RequestTimeoutMS int
	EventID          string
	EventType        string
	Body             []byte
}

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

func (c *Client) Send(ctx context.Context, reqData Request) attempt.Result {
	start := time.Now()

	timeout := time.Duration(reqData.RequestTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		reqData.URL,
		bytes.NewReader(reqData.Body),
	)
	if err != nil {
		code := "REQUEST_BUILD_ERROR"
		msg := err.Error()
		return attempt.Result{Status: attempt.StatusPermanent, ErrorCode: &code, ErrorMessage: &msg, Latency: time.Since(start)}
	}

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Hookyard-Delivery/1.0")
	req.Header.Set("X-Hookyard-Event-ID", reqData.EventID)
	req.Header.Set("X-Hookyard-Event-Type", reqData.EventType)
	req.Header.Set("X-Hookyard-Timestamp", timestamp)
	req.Header.Set("X-Hookyard-Signature", Sign(reqData.Secret, timestamp, reqData.Body))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		code := "HTTP_ERROR"
		if requestCtx.Err() == context.DeadlineExceeded {
			code = "TIMEOUT"
		}
		msg := err.Error()
		return attempt.Result{Status: attempt.StatusRetryable, ErrorCode: &code, ErrorMessage: &msg, Latency: time.Since(start)}
	}
	defer resp.Body.Close()

	const maxResponseBody = 64 * 1024
	responseBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if readErr != nil {
		code := "RESPONSE_READ_ERROR"
		msg := readErr.Error()
		return attempt.Result{Status: attempt.StatusRetryable, ErrorCode: &code, ErrorMessage: &msg, Latency: time.Since(start)}
	}

	statusCode := resp.StatusCode
	headers := map[string]string{}
	for key, values := range resp.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	result := attempt.Result{
		HTTPStatusCode:  &statusCode,
		ResponseHeaders: headers,
		ResponseBody:    string(responseBytes),
		Latency:         time.Since(start),
	}

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		result.Status = attempt.StatusSuccess
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		result.Status = attempt.StatusRetryable
		code := fmt.Sprintf("HTTP_%d", resp.StatusCode)
		result.ErrorCode = &code
		msg := fmt.Sprintf("consumer returned HTTP %d", resp.StatusCode)
		result.ErrorMessage = &msg
	default:
		result.Status = attempt.StatusPermanent
		code := fmt.Sprintf("HTTP_%d", resp.StatusCode)
		result.ErrorCode = &code
		msg := fmt.Sprintf("consumer returned HTTP %d", resp.StatusCode)
		result.ErrorMessage = &msg
	}

	return result
}
