// Package httpapi holds the producer's HTTP layer. This file is the CORS /
// Origin policy: one allow-list, enforced on REST routes through withCORS and
// on the WebSocket upgrade through OriginChecker.
package httpapi

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// allowAllMarker is the allow-list entry meaning "trust every origin". It is
// what an unset CORS_ALLOWED_ORIGINS resolves to (zero-config demo default),
// so it must stay defined in exactly one place: a marker that differs by even
// one character silently flips that default from allow-all to deny-all.
const allowAllMarker = "*"

// AllowsAll reports whether the allow-list trusts every origin.
func AllowsAll(allowed map[string]struct{}) bool {
	_, ok := allowed[allowAllMarker]
	return ok
}

// LoadAllowedOrigins reads the CORS allow-list. CORS_ALLOWED_ORIGINS is the
// current name; LOADTEST_ALLOWED_ORIGINS is still honored because it came
// first, back when the list was only ever intended for the /ops/loadtest
// routes. It now governs every route, so the name is worth migrating off.
func LoadAllowedOrigins() map[string]struct{} {
	if raw := strings.TrimSpace(os.Getenv("CORS_ALLOWED_ORIGINS")); raw != "" {
		return parseAllowedOrigins(raw)
	}
	if raw := strings.TrimSpace(os.Getenv("LOADTEST_ALLOWED_ORIGINS")); raw != "" {
		slog.Warn("LOADTEST_ALLOWED_ORIGINS is deprecated — rename to CORS_ALLOWED_ORIGINS; " +
			"it now applies to every route, not just /ops/loadtest")
		return parseAllowedOrigins(raw)
	}
	return parseAllowedOrigins("")
}

// OriginChecker adapts the HTTP allow-list to gorilla's WebSocket upgrader
// so both transports enforce one policy.
func OriginChecker(allowedOrigins map[string]struct{}) func(*http.Request) bool {
	return func(r *http.Request) bool {
		return isRequestOriginAllowed(r, allowedOrigins)
	}
}

func withCORS(next http.Handler, allowedOrigins map[string]struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}
		if !isRequestOriginAllowed(r, allowedOrigins) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}

		w.Header().Add("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Admin-Key")
		w.Header().Set("Access-Control-Max-Age", "600")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func parseAllowedOrigins(raw string) map[string]struct{} {
	allowed := make(map[string]struct{})
	if strings.TrimSpace(raw) == "" {
		// Zero-config demo mode: if no explicit allow-list is configured,
		// trust all origins. Set LOADTEST_ALLOWED_ORIGINS to a comma-separated
		// list in stricter environments.
		allowed[allowAllMarker] = struct{}{}
		return allowed
	}

	for _, token := range strings.Split(raw, ",") {
		origin := strings.TrimSpace(token)
		if origin == "" {
			continue
		}
		if origin == allowAllMarker {
			allowed[allowAllMarker] = struct{}{}
			continue
		}
		key, _, _, _, ok := normalizeOrigin(origin)
		if !ok {
			slog.Warn("ignoring invalid allowed origin", "origin", origin)
			continue
		}
		allowed[key] = struct{}{}
	}
	return allowed
}

func isRequestOriginAllowed(r *http.Request, allowedOrigins map[string]struct{}) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	if _, ok := allowedOrigins[allowAllMarker]; ok {
		return true
	}

	originKey, originScheme, originHost, originPort, ok := normalizeOrigin(origin)
	if !ok {
		return false
	}

	reqScheme := requestScheme(r)
	reqHost, reqPort, ok := normalizeHostPort(r.Host, reqScheme)
	if ok &&
		originScheme == reqScheme &&
		originHost == reqHost &&
		originPort == reqPort {
		return true
	}

	_, ok = allowedOrigins[originKey]
	return ok
}

func normalizeOrigin(raw string) (key, scheme, host, port string, ok bool) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", "", "", "", false
	}

	scheme = strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", "", "", "", false
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", "", "", false
	}
	if path := strings.TrimSpace(parsed.Path); path != "" && path != "/" {
		return "", "", "", "", false
	}

	host = strings.ToLower(parsed.Hostname())
	if host == "" {
		return "", "", "", "", false
	}
	port = parsed.Port()
	if port == "" {
		port = defaultPortForScheme(scheme)
	}
	if port == "" {
		return "", "", "", "", false
	}

	key = fmt.Sprintf("%s://%s:%s", scheme, host, port)
	return key, scheme, host, port, true
}

func normalizeHostPort(hostport, scheme string) (host, port string, ok bool) {
	hostURL, err := url.Parse("http://" + strings.TrimSpace(hostport))
	if err != nil {
		return "", "", false
	}
	host = strings.ToLower(hostURL.Hostname())
	if host == "" {
		return "", "", false
	}
	port = hostURL.Port()
	if port == "" {
		port = defaultPortForScheme(scheme)
	}
	if port == "" {
		return "", "", false
	}
	return host, port, true
}

func requestScheme(r *http.Request) string {
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); forwarded != "" {
		if idx := strings.Index(forwarded, ","); idx >= 0 {
			forwarded = strings.TrimSpace(forwarded[:idx])
		}
		switch strings.ToLower(forwarded) {
		case "http", "https":
			return strings.ToLower(forwarded)
		}
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func defaultPortForScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "https":
		return "443"
	case "http":
		return "80"
	default:
		return ""
	}
}
