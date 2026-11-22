package amqp_helper

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestBuildTLSConfigWarnsOnlyInRequireMode(t *testing.T) {
	cases := map[string]struct {
		insecure bool
		tls      bool
		warns    int
	}{
		"require":     {insecure: true, tls: true, warns: 1},
		"verify-ca":   {insecure: false, tls: true, warns: 0},
		"verify-full": {insecure: false, tls: true, warns: 0},
		"disable":     {tls: false, warns: 0},
	}
	for mode, tc := range cases {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("AMQP_SSLMODE", mode)
			t.Setenv("AMQP_HOST", "rabbitmq")
			requireModeWarning = sync.Once{}
			var logs bytes.Buffer
			logger := logrus.New()
			logger.SetFormatter(&logrus.JSONFormatter{})
			logger.SetOutput(&logs)

			cfg := buildTLSConfig(logger)
			if (cfg != nil) != tc.tls {
				t.Fatalf("TLS enabled = %v, want %v", cfg != nil, tc.tls)
			}
			if cfg != nil && cfg.InsecureSkipVerify != tc.insecure {
				t.Fatalf("InsecureSkipVerify = %v, want %v", cfg.InsecureSkipVerify, tc.insecure)
			}

			out := strings.TrimSpace(logs.String())
			var lines []string
			if out != "" {
				lines = strings.Split(out, "\n")
			}
			if len(lines) != tc.warns {
				t.Fatalf("got %d log line(s), want %d: %q", len(lines), tc.warns, out)
			}
			if tc.warns == 0 {
				return
			}
			var entry map[string]any
			if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
				t.Fatalf("line %q is not JSON: %v", lines[0], err)
			}
			if entry["level"] != "warning" || entry["component"] != "amqp" || entry["ssl_mode"] != "require" {
				t.Fatalf("unexpected warning fields: %v", entry)
			}
			if !strings.Contains(entry["msg"].(string), "verify-ca") {
				t.Fatalf("warning does not advise verify-ca: %q", entry["msg"])
			}
		})
	}
}

// Dials an unreachable broker twice per mode and counts warnings on the standard logger.
func TestDialWarnsOncePerProcessInRequireMode(t *testing.T) {
	for mode, want := range map[string]int{"require": 1, "verify-ca": 0, "disable": 0} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("AMQP_SSLMODE", mode)
			t.Setenv("AMQP_HOST", "127.0.0.1")
			requireModeWarning = sync.Once{}
			var logs bytes.Buffer
			std := logrus.StandardLogger()
			prevOut, prevFmt := std.Out, std.Formatter
			std.SetOutput(&logs)
			std.SetFormatter(&logrus.JSONFormatter{})
			t.Cleanup(func() { std.SetOutput(prevOut); std.SetFormatter(prevFmt) })

			for range 2 {
				if conn, err := Dial("amqps://guest:guest@127.0.0.1:1/"); err == nil {
					_ = conn.Close()
					t.Fatal("dial to a closed port succeeded")
				}
			}
			if got := strings.Count(logs.String(), "require mode"); got != want {
				t.Fatalf("got %d require-mode warning(s), want %d: %q", got, want, logs.String())
			}
		})
	}
}
