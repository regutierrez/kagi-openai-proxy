package config

import (
	"reflect"
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
	if !reflect.DeepEqual(s, want) {
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

func TestAllowedHosts(t *testing.T) {
	base := func(raw *string) Lookup {
		vars := map[string]string{"KAGI_SESSION_TOKEN": "tok", "PROXY_API_KEY": strings.Repeat("k", 16)}
		if raw != nil {
			vars["PROXY_ALLOWED_HOSTS"] = *raw
		}
		return env(vars)
	}
	str := func(s string) *string { return &s }
	for _, c := range []struct {
		raw  *string
		want []string
	}{
		{nil, []string{"127.0.0.1", "localhost"}},     // unset: localhost-only default
		{str(""), []string{"127.0.0.1", "localhost"}}, // empty: default
		{str(" , "), []string{"127.0.0.1", "localhost"}},
		{str(" 100.101.102.103:8000 , mymac.tail1234.ts.net,[fd7a:115c::1]:8000 "),
			[]string{"100.101.102.103", "mymac.tail1234.ts.net", "[fd7a:115c::1]"}},
	} {
		s, err := FromEnv(base(c.raw))
		if err != nil || !reflect.DeepEqual(s.AllowedHosts, c.want) {
			t.Fatalf("PROXY_ALLOWED_HOSTS=%v -> %v, %v; want %v", c.raw, s.AllowedHosts, err, c.want)
		}
	}
	if _, err := FromEnv(base(str("good.host, bad host"))); err == nil {
		t.Fatal("invalid entry accepted")
	}
}

func TestReasoningEffortEnv(t *testing.T) {
	load := func(vars map[string]string) (Settings, error) {
		vars["KAGI_SESSION_TOKEN"], vars["PROXY_API_KEY"] = "tok", strings.Repeat("k", 16)
		return FromEnv(env(vars))
	}
	if s, err := load(map[string]string{}); err != nil || s.ReasoningEffort != "" {
		t.Fatalf("unset: %q %v", s.ReasoningEffort, err)
	}
	for raw, want := range map[string]string{"low": "low", " MAX ": "max", "": "", "none": "none"} {
		if s, err := load(map[string]string{"KAGI_REASONING_EFFORT": raw}); err != nil || s.ReasoningEffort != want {
			t.Fatalf("%q -> %q %v", raw, s.ReasoningEffort, err)
		}
	}
	_, err := load(map[string]string{"KAGI_REASONING_EFFORT": "turbo"})
	if err == nil || !strings.Contains(err.Error(), "KAGI_REASONING_EFFORT must be one of none, minimal, low, medium, high, xhigh, max") {
		t.Fatalf("err = %v", err)
	}
}
