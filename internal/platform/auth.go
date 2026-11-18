package platform

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"
)

type User struct {
	ID   string `json:"id"`
	Role string `json:"role"`
	Name string `json:"name"`
}
type userKey struct{}

func token() string {
	b := make([]byte, 32)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func hash(s string) string         { b := sha256.Sum256([]byte(s)); return hex.EncodeToString(b[:]) }
func current(r *http.Request) User { u, _ := r.Context().Value(userKey{}).(User); return u }
func (a *App) authenticate(r *http.Request) (User, error) {
	key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if key == "" {
		if c, e := r.Cookie("tf_session"); e == nil {
			key = c.Value
		}
	}
	if key == "" {
		return User{}, errForbidden
	}
	if subtle.ConstantTimeCompare([]byte(hash(key)), []byte(hash(a.cfg.AdminKey))) == 1 {
		return User{"bootstrap-admin", "administrator", "Administrator"}, nil
	}
	return a.store.KeyByDigest(r.Context(), hash(key))
}
func (a *App) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, e := a.authenticate(r)
		if e != nil {
			if strings.HasPrefix(r.URL.Path, "/ui") {
				http.Redirect(w, r, "/login", 303)
			} else {
				fail(w, 401, "Valid Bearer API key required")
			}
			return
		}
		// Browser mutations require a session-bound CSRF token.
		if r.Method != "GET" && r.Method != "HEAD" && r.Header.Get("Authorization") == "" {
			csrf := r.Header.Get("X-CSRF-Token")
			if csrf == "" {
				csrf = r.FormValue("csrf")
			}
			c, _ := r.Cookie("tf_session")
			if c == nil || subtle.ConstantTimeCompare([]byte(csrf), []byte(hash(c.Value+"csrf"))) != 1 {
				fail(w, 403, "Invalid CSRF token")
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey{}, u)))
	})
}
func require(w http.ResponseWriter, r *http.Request, roles ...string) bool {
	for _, role := range roles {
		if current(r).Role == role {
			return true
		}
	}
	fail(w, 403, "Insufficient role")
	return false
}
func writeAccess(w http.ResponseWriter, r *http.Request) bool {
	return require(w, r, "administrator", "developer")
}
