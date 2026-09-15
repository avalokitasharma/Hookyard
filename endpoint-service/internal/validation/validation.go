package validation

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

var eventTypeRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,254}$`)

func EndpointName(v string) error {
	v = strings.TrimSpace(v)
	if v == "" || len(v) > 255 {
		return fmt.Errorf("name must be between 1 and 255 characters")
	}
	return nil
}

func WebhookURL(raw string, allowHTTP bool) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("invalid webhook URL")
	}
	if u.User != nil {
		return fmt.Errorf("webhook URL must not contain userinfo")
	}
	if u.Scheme != "https" && !(allowHTTP && u.Scheme == "http") {
		return fmt.Errorf("webhook URL must use https")
	}
	if strings.ContainsAny(u.Host, "\\\r\n\t") {
		return fmt.Errorf("invalid webhook host")
	}
	return nil
}

func EventTypes(types []string) ([]string, error) {
	if len(types) == 0 {
		return nil, fmt.Errorf("at least one event type is required")
	}
	seen := make(map[string]struct{}, len(types))
	out := make([]string, 0, len(types))
	for _, raw := range types {
		v := strings.TrimSpace(raw)
		if !eventTypeRE.MatchString(v) {
			return nil, fmt.Errorf("invalid event type %q", raw)
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out, nil
}

func Timeout(ms int, field string) error {
	if ms < 100 || ms > 120000 {
		return fmt.Errorf("%s must be between 100 and 120000 ms", field)
	}
	return nil
}
