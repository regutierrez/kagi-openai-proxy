// Command kagi-openai-proxy runs the OpenAI-compatible proxy for Kagi Assistant on
// PROXY_HOST:PROXY_PORT (default 127.0.0.1:8000).
//
// With -healthcheck it instead probes GET /health on that address and exits 0 (healthy)
// or 1, for container HEALTHCHECKs on images without curl.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"kagiopenaiproxy/internal/config"
	"kagiopenaiproxy/internal/logx"
	"kagiopenaiproxy/internal/server"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe GET /health on PROXY_HOST:PROXY_PORT and exit 0 (healthy) or 1")
	flag.Parse()
	settings, err := config.FromEnv(os.LookupEnv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kagi-openai-proxy: configuration error: %v\n", err)
		os.Exit(1)
	}
	if *healthcheck {
		if err := server.HealthCheck(settings, 3*time.Second); err != nil {
			fmt.Fprintf(os.Stderr, "kagi-openai-proxy: healthcheck failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	for _, warning := range settings.Warnings {
		logx.Warn("%s", warning)
	}
	if !server.IsLoopback(settings.Host) {
		logx.Warn("PROXY_HOST=%s is not a loopback address: make sure this port is reachable only from trusted networks (e.g. published on a Tailscale IP), never the internet", settings.Host)
	}
	proxy := server.New(settings)
	srv := &http.Server{
		Addr:              net.JoinHostPort(settings.Host, strconv.Itoa(settings.Port)),
		Handler:           proxy,
		ReadHeaderTimeout: 20 * time.Second,
		IdleTimeout:       120 * time.Second,
		// No WriteTimeout: completions stream for minutes.
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	logx.Info("listening on http://%s (version %s)", srv.Addr, server.Version)

	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logx.Error("server failed: %v", err)
			proxy.Close()
			os.Exit(1)
		}
	case <-ctx.Done():
		logx.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close() // streams still open after 10s: cut them; their turns drain (bounded)
		}
		cancel()
	}
	proxy.Close()
}
