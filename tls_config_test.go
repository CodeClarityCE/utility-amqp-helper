package amqp_helper

import (
	"bytes"
	"encoding/json"
	"strings"
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
