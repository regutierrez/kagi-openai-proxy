package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"kagiopenaiproxy/internal/config"
)

// HealthCheck is the container health probe (the -healthcheck flag): GET /health on the
// configured address, nil only for 200 {"status":"ok"}. An unspecified bind address
// (0.0.0.0, ::) is reached through loopback. The Host header is the first allowed host,
// so the probe passes the allow-list whatever PROXY_ALLOWED_HOSTS is. Proxy environment
// variables are ignored. It makes no Kagi call.
func HealthCheck(s config.Settings, timeout time.Duration) error {
	host := s.Host
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		host = "127.0.0.1"
		if ip.To4() == nil {
			host = "::1"
		}
	}
	req, err := http.NewRequest(http.MethodGet, "http://"+net.JoinHostPort(host, strconv.Itoa(s.Port))+"/health", nil)
	if err != nil {
		return err
	}
	if len(s.AllowedHosts) > 0 {
		req.Host = s.AllowedHosts[0]
	}
	client := &http.Client{Timeout: timeout, Transport: &http.Transport{Proxy: nil}}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var health struct {
		Status string `json:"status"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &health) != nil || health.Status != "ok" {
		return fmt.Errorf("unhealthy: HTTP %d %s", resp.StatusCode, body)
	}
	return nil
}
