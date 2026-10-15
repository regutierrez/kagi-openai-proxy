# syntax=docker/dockerfile:1

# ---- build: static binary, no cgo
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kagi-openai-proxy ./cmd/kagi-openai-proxy

# ---- run: nothing but the binary and the CA bundle (needed for HTTPS to Kagi)
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/kagi-openai-proxy /kagi-openai-proxy
# Inside the container the proxy must listen on all interfaces; restrict exposure where
# the port is published (see docker-compose.yml) and with PROXY_ALLOWED_HOSTS.
ENV PROXY_HOST=0.0.0.0 \
    PROXY_PORT=8000
USER 65534:65534
EXPOSE 8000
# No curl on scratch: the binary probes its own /health (no Kagi call).
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["/kagi-openai-proxy", "-healthcheck"]
ENTRYPOINT ["/kagi-openai-proxy"]
