// Package config reads the proxy's settings from environment variables.
package config

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"kagiopenaiproxy/internal/textutil"
)

// MinProxyKeyLength rejects weak keys; redaction would also mangle ordinary words
// that contain a short key.
const MinProxyKeyLength = 16

// Settings is the full proxy configuration. Field defaults live in Defaults().
type Settings struct {
	KagiSessionToken string
	ProxyAPIKey      string
	Host             string
	// AllowedHosts are the accepted Host header values, ports stripped (DNS-rebinding
	// defence). PROXY_ALLOWED_HOSTS overrides DefaultAllowedHosts.
	AllowedHosts        []string
	Port                int
	KagiBaseURL         string
	DeleteThreads       bool
	Personalization     bool
	SearchDefaultOnline bool
	MaxConcurrency      int
	QueueWait           time.Duration
	ConnectTimeout      time.Duration
	ReadTimeout         time.Duration
	ModelsTTL           time.Duration
	// Internal tuning knobs; not part of the documented env surface.
	Keepalive    time.Duration
	DrainTimeout time.Duration
}

// String never prints the secrets.
func (s Settings) String() string {
	return fmt.Sprintf("Settings(host=%s, port=%d, kagi_base_url=%s)", s.Host, s.Port, s.KagiBaseURL)
}

// Defaults returns the settings used when no environment variable overrides them.
func Defaults(token, key string) Settings {
	return Settings{
		KagiSessionToken:    token,
		ProxyAPIKey:         key,
		Host:                "127.0.0.1",
		AllowedHosts:        DefaultAllowedHosts(),
		Port:                8000,
		KagiBaseURL:         "https://assistant.kagi.com",
		DeleteThreads:       true,
		Personalization:     false,
		SearchDefaultOnline: true,
		MaxConcurrency:      1,
		QueueWait:           30 * time.Second,
		ConnectTimeout:      20 * time.Second,
		ReadTimeout:         120 * time.Second,
		ModelsTTL:           21600 * time.Second,
		Keepalive:           15 * time.Second,
		DrainTimeout:        600 * time.Second,
	}
}

// DefaultAllowedHosts is the localhost-only Host allow-list.
func DefaultAllowedHosts() []string { return []string{"127.0.0.1", "localhost"} }

// hostRE is Starlette's host-header grammar: a name, IPv4, or bracketed IPv6 address,
// optionally followed by :port.
var hostRE = regexp.MustCompile(`(?i)^([a-z0-9._~%!$&'()*+,;=-]+|\[(?:[a-f0-9]*:[a-f0-9.:]+|(?-i:v)[a-f0-9]+\.[a-z0-9._~!$&'()*+,;=:-]+)\])(?::[0-9]+)?$`)

// HostOnly returns the host part of a Host header value (port stripped; IPv6 keeps its
// brackets). ok is false if the value is not a valid host.
func HostOnly(hostHeader string) (host string, ok bool) {
	m := hostRE.FindStringSubmatch(hostHeader)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// parseAllowedHosts reads a comma-separated host list; whitespace and ports are stripped.
func parseAllowedHosts(raw string) ([]string, error) {
	var hosts []string
	for _, entry := range strings.Split(raw, ",") {
		entry = textutil.Strip(entry)
		if entry == "" {
			continue
		}
		host, ok := HostOnly(entry)
		if !ok {
			return nil, errorf("PROXY_ALLOWED_HOSTS entry %s is not a host name or address", textutil.Repr(entry))
		}
		hosts = append(hosts, host)
	}
	if len(hosts) == 0 {
		return DefaultAllowedHosts(), nil
	}
	return hosts, nil
}

// Error is an invalid or missing setting.
type Error struct{ msg string }

func (e *Error) Error() string { return e.msg }

func errorf(format string, args ...any) error { return &Error{fmt.Sprintf(format, args...)} }

// Lookup reads one variable; ok is false when it is unset (os.LookupEnv fits).
type Lookup func(name string) (value string, ok bool)

// FromEnv builds Settings from environment variables.
func FromEnv(get Lookup) (Settings, error) {
	token, _ := get("KAGI_SESSION_TOKEN")
	key, _ := get("PROXY_API_KEY")
	token, key = textutil.Strip(token), textutil.Strip(key)
	var missing []string
	if token == "" {
		missing = append(missing, "KAGI_SESSION_TOKEN")
	}
	if key == "" {
		missing = append(missing, "PROXY_API_KEY")
	}
	if len(missing) > 0 {
		return Settings{}, errorf("required environment variable(s) not set: %s", strings.Join(missing, ", "))
	}
	if textutil.Len(key) < MinProxyKeyLength {
		return Settings{}, errorf("PROXY_API_KEY must be at least %d characters long", MinProxyKeyLength)
	}
	search := "online"
	if raw, ok := get("KAGI_SEARCH_DEFAULT"); ok {
		if v := strings.ToLower(textutil.Strip(raw)); v != "" {
			search = v
		}
	}
	if search != "online" && search != "offline" {
		return Settings{}, errorf("KAGI_SEARCH_DEFAULT must be 'online' or 'offline', got %s", textutil.Repr(search))
	}

	s := Defaults(token, key)
	s.SearchDefaultOnline = search == "online"
	if raw, ok := get("PROXY_HOST"); ok && textutil.Strip(raw) != "" {
		s.Host = textutil.Strip(raw)
	}
	if raw, ok := get("PROXY_ALLOWED_HOSTS"); ok {
		hosts, err := parseAllowedHosts(raw)
		if err != nil {
			return Settings{}, err
		}
		s.AllowedHosts = hosts
	}
	if raw, ok := get("KAGI_BASE_URL"); ok && textutil.Strip(raw) != "" {
		s.KagiBaseURL = textutil.Strip(raw)
	}
	s.KagiBaseURL = strings.TrimRight(s.KagiBaseURL, "/")

	var err error
	if s.Port, err = integer(get, "PROXY_PORT", s.Port, 1); err != nil {
		return Settings{}, err
	}
	if s.DeleteThreads, err = boolean(get, "KAGI_DELETE_THREADS", s.DeleteThreads); err != nil {
		return Settings{}, err
	}
	if s.Personalization, err = boolean(get, "KAGI_PERSONALIZATION", s.Personalization); err != nil {
		return Settings{}, err
	}
	if s.MaxConcurrency, err = integer(get, "KAGI_MAX_CONCURRENCY", s.MaxConcurrency, 1); err != nil {
		return Settings{}, err
	}
	for _, d := range []struct {
		name string
		dst  *time.Duration
	}{
		{"KAGI_QUEUE_WAIT_SECONDS", &s.QueueWait},
		{"KAGI_CONNECT_TIMEOUT", &s.ConnectTimeout},
		{"KAGI_READ_TIMEOUT", &s.ReadTimeout},
		{"KAGI_MODELS_TTL", &s.ModelsTTL},
	} {
		if *d.dst, err = seconds(get, d.name, *d.dst); err != nil {
			return Settings{}, err
		}
	}
	return s, nil
}

func boolean(get Lookup, name string, def bool) (bool, error) {
	raw, ok := get(name)
	if !ok || textutil.Strip(raw) == "" {
		return def, nil
	}
	switch strings.ToLower(textutil.Strip(raw)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	}
	return false, errorf("%s must be true/false, got %s", name, textutil.Repr(raw))
}

func integer(get Lookup, name string, def, minimum int) (int, error) {
	raw, ok := get(name)
	if !ok || textutil.Strip(raw) == "" {
		return def, nil
	}
	v, err := strconv.Atoi(textutil.Strip(raw))
	if err != nil {
		return 0, errorf("%s must be an integer, got %s", name, textutil.Repr(raw))
	}
	if v < minimum {
		return 0, errorf("%s must be >= %d, got %s", name, minimum, textutil.Repr(raw))
	}
	return v, nil
}

func seconds(get Lookup, name string, def time.Duration) (time.Duration, error) {
	raw, ok := get(name)
	if !ok || textutil.Strip(raw) == "" {
		return def, nil
	}
	v, err := strconv.ParseFloat(textutil.Strip(raw), 64)
	if err != nil {
		return 0, errorf("%s must be a number, got %s", name, textutil.Repr(raw))
	}
	if v < 0 {
		return 0, errorf("%s must be >= 0, got %s", name, textutil.Repr(raw))
	}
	return time.Duration(v * float64(time.Second)), nil
}
