package config

import (
	"strings"
	"testing"
	"time"
)

func env(vars map[string]string) Lookup {
	return func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	}
}

func TestShortProxyKeyRejectedAtStartup(t *testing.T) {
	for _, key := range []string{"test", strings.Repeat("x", 15)} {
		_, err := FromEnv(env(map[string]string{"KAGI_SESSION_TOKEN": "session-token", "PROXY_API_KEY": key}))
		if err == nil || !strings.Contains(err.Error(), "at least 16 characters") {
			t.Fatalf("key %q: err = %v", key, err)
		}
	}
	s, err := FromEnv(env(map[string]string{"KAGI_SESSION_TOKEN": "session-token", "PROXY_API_KEY": strings.Repeat("x", 16)}))
	if err != nil || s.ProxyAPIKey != strings.Repeat("x", 16) {
		t.Fatalf("16-char key: %v", err)
	}
}

func TestRequiredVariables(t *testing.T) {
	_, err := FromEnv(env(nil))
	if err == nil || err.Error() != "required environment variable(s) not set: KAGI_SESSION_TOKEN, PROXY_API_KEY" {
		t.Fatalf("err = %v", err)
	}
}

func TestDefaultsAndOverrides(t *testing.T) {
	base := map[string]string{"KAGI_SESSION_TOKEN": "tok", "PROXY_API_KEY": strings.Repeat("k", 16)}
	s, err := FromEnv(env(base))
	if err != nil {
		t.Fatal(err)
	}
	want := Defaults("tok", strings.Repeat("k", 16))
	if s != want {
		t.Fatalf("defaults = %+v\nwant %+v", s, want)
	}
	for k, v := range map[string]string{
		"PROXY_HOST": "localhost", "PROXY_PORT": "9000", "KAGI_BASE_URL": "http://x/", "KAGI_DELETE_THREADS": "no",
		"KAGI_PERSONALIZATION": "on", "KAGI_SEARCH_DEFAULT": "OFFLINE", "KAGI_MAX_CONCURRENCY": "2",
		"KAGI_QUEUE_WAIT_SECONDS": "0.5", "KAGI_CONNECT_TIMEOUT": "5", "KAGI_READ_TIMEOUT": "60", "KAGI_MODELS_TTL": "0",
	} {
		base[k] = v
	}
	s, err = FromEnv(env(base))
	if err != nil {
		t.Fatal(err)
	}
	if s.Host != "localhost" || s.Port != 9000 || s.KagiBaseURL != "http://x" || s.DeleteThreads || !s.Personalization ||
		s.SearchDefaultOnline || s.MaxConcurrency != 2 || s.QueueWait != 500*time.Millisecond || s.ConnectTimeout != 5*time.Second ||
		s.ReadTimeout != time.Minute || s.ModelsTTL != 0 {
		t.Fatalf("overrides = %+v", s)
	}
	for name, bad := range map[string]string{"KAGI_DELETE_THREADS": "maybe", "PROXY_PORT": "0", "KAGI_SEARCH_DEFAULT": "sometimes", "KAGI_READ_TIMEOUT": "-1"} {
		vars := map[string]string{"KAGI_SESSION_TOKEN": "tok", "PROXY_API_KEY": strings.Repeat("k", 16), name: bad}
		if _, err := FromEnv(env(vars)); err == nil {
			t.Fatalf("%s=%s accepted", name, bad)
		}
	}
}

func TestStringHidesSecrets(t *testing.T) {
	s := Defaults("the-token", "the-key-0123456789")
	if out := s.String(); strings.Contains(out, "the-token") || strings.Contains(out, "the-key") {
		t.Fatalf("String leaks secrets: %s", out)
	}
}
