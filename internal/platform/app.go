package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	MongoURI, MongoDatabase, AdminKey, AIURL, ServiceKey, Addr string
	RedisURL, RedisNamespace string
	Concurrency, QueueSize                         int
	QueueTimeout, JobTimeout                       time.Duration
	SecureCookies                                  bool
	WriteApproval                                  bool
}

func EnvConfig() (Config, error) {
	c := Config{MongoURI: env("MONGODB_URI", "mongodb://localhost:27017"), MongoDatabase: env("TF_MONGO_DATABASE", "tensorforge"), AdminKey: os.Getenv("TF_ADMIN_KEY"), ServiceKey: os.Getenv("TF_SERVICE_KEY"), AIURL: env("TF_AI_URL", "http://ai:8101"), Addr: env("TF_ADDR", ":8000"), Concurrency: integer("TF_CONCURRENCY", 1), QueueSize: integer("TF_QUEUE_SIZE", 8), QueueTimeout: 30 * time.Second, JobTimeout: 2 * time.Hour, SecureCookies: os.Getenv("TF_SECURE_COOKIES") == "true", WriteApproval: env("TF_WRITE_APPROVAL", "true") != "false"}
	if len(c.AdminKey) < 32 || len(c.ServiceKey) < 32 {
		return c, errors.New("TF_ADMIN_KEY and TF_SERVICE_KEY (>=32 characters) required")
	}
	if c.Concurrency < 1 || c.Concurrency > 16 || c.QueueSize < 0 || c.QueueSize > 100 {
		return c, errors.New("invalid concurrency/queue limits")
	}
	c.RedisURL = env("REDIS_URL", "redis://localhost:6379/0")
	c.RedisNamespace = env("TF_REDIS_NAMESPACE", "tensorforge")
	return c, nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func integer(k string, d int) int {
	n, e := strconv.Atoi(os.Getenv(k))
	if e != nil {
		return d
	}
	return n
}

type App struct {
	cfg            Config
	store          *Store
	vectors        *VectorStore
	client         *http.Client
	slots, waiting chan struct{}
	mu             sync.Mutex
	cancel         map[string]context.CancelFunc
	wake           chan struct{}
	templates      *Templates
	lifetime       context.Context
	shutdown       context.CancelFunc
	leaseLost      atomic.Bool
	stopWorker     context.CancelFunc
	workerDone     sync.WaitGroup
}

func New(ctx context.Context, c Config) (*App, error) {
	s, e := OpenStore(ctx, c.MongoURI, c.MongoDatabase)
	if e != nil {
		return nil, e
	}
	if e = s.AcquireLease(ctx); e != nil {
		s.Close(context.Background())
		return nil, e
	}
	lifetime, shutdown := context.WithCancel(ctx)
	a := &App{cfg: c, store: s, client: &http.Client{Timeout: 0}, slots: make(chan struct{}, c.Concurrency), waiting: make(chan struct{}, c.Concurrency+c.QueueSize), cancel: map[string]context.CancelFunc{}, wake: make(chan struct{}, 1), lifetime: lifetime, shutdown: shutdown}
	a.vectors, e = OpenVectorStore(ctx, c.RedisURL, c.RedisNamespace)
	if e != nil { a.Close(); return nil, e }
	if e = s.Recover(ctx); e != nil {
		a.Close()
		return nil, e
	}
	resetCtx, resetCancel := context.WithTimeout(ctx, 10*time.Second)
	if _, resetErr := a.ai(resetCtx, "/reset", map[string]any{}); resetErr != nil {
		slog.Warn("AI reconciliation unavailable", "error", resetErr)
	}
	resetCancel()
	a.templates, e = loadTemplates()
	if e != nil {
		a.Close()
		return nil, e
	}
	return a, nil
}
func (a *App) Close() {
	if a.shutdown != nil { a.shutdown() }
	if a.stopWorker != nil { a.stopWorker() }
	a.workerDone.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a.store.ReleaseLease(ctx)
	if a.vectors != nil { a.vectors.Client.Close() }
	a.client.CloseIdleConnections()
	a.store.Close(ctx)
}
func (a *App) Run(ctx context.Context) {
	context.AfterFunc(ctx, a.shutdown)
	ctx, a.stopWorker = context.WithCancel(a.lifetime)
	a.workerDone.Add(2)
	go func() { defer a.workerDone.Done(); a.worker(ctx) }()
	go func() {
		defer a.workerDone.Done()
		ticker := time.NewTicker(5*time.Second)
		defer ticker.Stop()
		for {
			select { case <-ctx.Done(): return; case <-ticker.C: }
			check, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := a.store.RenewLease(check)
			cancel()
			if err != nil {
				if ctx.Err() != nil { return }
				slog.Error("scheduler lease lost", "error", err)
				a.leaseLost.Store(true)
				a.shutdown()
				return
			}
			flush, done := context.WithTimeout(ctx, 5*time.Second)
			if err := a.store.FlushAudit(flush); err != nil { slog.Warn("audit outbox flush", "error", err) }
			done()
		}
	}()
}
func (a *App) Handler() http.Handler {
	api := http.NewServeMux()
	api.HandleFunc("GET /api/me", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, current(r)) })
	api.HandleFunc("GET /api/gpu", a.gpu)
	api.HandleFunc("GET /api/keys", a.keys)
	api.HandleFunc("POST /api/keys", a.keys)
	api.HandleFunc("DELETE /api/keys/{id}", a.revokeKey)
	api.HandleFunc("GET /api/models", a.models)
	api.HandleFunc("POST /api/models", a.models)
	api.HandleFunc("POST /api/models/{id}/{action}", a.modelAction)
	api.HandleFunc("GET /v1/models", a.openaiModels)
	api.HandleFunc("POST /v1/chat/completions", a.chat)
	api.HandleFunc("GET /api/jobs", a.jobs)
	api.HandleFunc("GET /api/jobs/{id}", a.job)
	api.HandleFunc("GET /api/jobs/{id}/artifacts/{path...}", a.artifact)
	api.HandleFunc("PUT /api/jobs/{id}/acl", a.jobACL)
	api.HandleFunc("POST /api/jobs/{id}/cancel", a.cancelJob)
	api.HandleFunc("GET /api/documents", a.documents)
	api.HandleFunc("POST /api/documents", a.documents)
	api.HandleFunc("POST /api/documents/{id}/reindex", a.reindexDocument)
	api.HandleFunc("PUT /api/documents/{id}/acl", a.documentACL)
	api.HandleFunc("POST /api/retrieval/search", a.search)
	api.HandleFunc("POST /api/retrieval/answer", a.answer)
	api.HandleFunc("GET /api/datasets", a.datasets)
	api.HandleFunc("POST /api/datasets", a.datasets)
	api.HandleFunc("POST /api/training", a.train)
	api.HandleFunc("POST /api/evaluations", a.evaluate)
	api.HandleFunc("POST /api/benchmarks", a.benchmark)
	api.HandleFunc("GET /api/agents", a.agents)
	api.HandleFunc("POST /api/agents", a.agents)
	api.HandleFunc("POST /api/agents/{id}/runs", a.agentRun)
	api.HandleFunc("GET /api/runs", a.runs)
	api.HandleFunc("POST /api/runs/{id}/approval", a.approval)
	api.HandleFunc("POST /mcp", a.mcp)
	api.HandleFunc("GET /api/audit", a.auditList)
	api.HandleFunc("GET /api/requests", a.requests)
	api.HandleFunc("GET /metrics", a.metrics)
	api.HandleFunc("GET /ui", a.dashboard)
	api.HandleFunc("GET /ui/panel/{kind}", a.panel)
	api.HandleFunc("GET /ui/jobs/{id}", a.jobProgress)
	api.HandleFunc("POST /ui/action/{action}", a.uiAction)
	root := http.NewServeMux()
	root.Handle("/api/", a.auth(api))
	root.Handle("/v1/", a.auth(api))
	root.Handle("/mcp", a.auth(api))
	root.Handle("/metrics", a.auth(api))
	root.Handle("/ui", a.auth(api))
	root.Handle("/ui/", a.auth(api))
	root.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "alive"}) })
	root.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if e := a.store.Ping(ctx); e != nil || a.leaseLost.Load() {
			fail(w, 503, "Database unavailable")
			return
		}
		if err := a.vectors.Client.Do(ctx,"FT.INFO",a.vectors.Index).Err(); err != nil { fail(w,503,"Redis vector index unavailable"); return }
		respond(w, 200, map[string]string{"status": "ready"})
	})
	root.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.Dir(env("TF_WEB_DIR", "web")+"/static"))))
	root.HandleFunc("GET /login", a.login)
	root.HandleFunc("POST /login", a.login)
	root.HandleFunc("POST /logout", a.auth(http.HandlerFunc(a.logout)).ServeHTTP)
	root.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/ui", 303) })
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.leaseLost.Load() && r.URL.Path != "/healthz" { fail(w, 503, "Scheduler lease lost; restart the gateway"); return }
		requestContext, cancel := context.WithCancel(r.Context())
		stop := context.AfterFunc(a.lifetime, cancel)
		defer stop()
		defer cancel()
		r = r.WithContext(requestContext)
		r.Body = http.MaxBytesReader(w, r.Body, 11<<20)
		id := token()[:24]
		w.Header().Set("X-Request-ID", id)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
		start := time.Now()
		defer func() {
			if v := recover(); v != nil {
				slog.Error("panic", "request_id", id, "error", v)
				fail(w, 500, "Internal error")
			}
			slog.Info("request", "request_id", id, "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(start).Milliseconds())
		}()
		root.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
	})
}

type requestIDKey struct{}

func requestID(r *http.Request) string { s, _ := r.Context().Value(requestIDKey{}).(string); return s }
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, msg string) {
	respond(w, status, map[string]any{"error": map[string]any{"message": msg, "type": http.StatusText(status), "code": status}})
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		fail(w, 422, e.Error())
		return false
	}
	if e := d.Decode(new(any)); e != io.EOF {
		fail(w, 422, "Expected one JSON object")
		return false
	}
	return true
}
func (a *App) ai(ctx context.Context, path string, input any) (map[string]any, error) {
	var body io.Reader
	if input != nil {
		b, e := json.Marshal(input)
		if e != nil {
			return nil, e
		}
		body = strings.NewReader(string(b))
	}
	method := "POST"
	if input == nil {
		method = "GET"
	}
	req, e := http.NewRequestWithContext(ctx, method, a.cfg.AIURL+path, body)
	if e != nil {
		return nil, e
	}
	req.Header.Set("Authorization", "Bearer "+a.cfg.ServiceKey)
	req.Header.Set("Content-Type", "application/json")
	resp, e := a.client.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if e != nil {
		return nil, e
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("AI service (%d): %s", resp.StatusCode, string(b))
	}
	var out map[string]any
	e = json.Unmarshal(b, &out)
	return out, e
}
func (a *App) list(w http.ResponseWriter, r *http.Request, kind string) {
	rows, e := a.store.List(r.Context(), kind)
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	out := []Record{}
	for _, row := range rows {
		if readable(row, current(r)) {
			out = append(out, publicRecord(row))
		}
	}
	respond(w, 200, out)
}
func accessError(w http.ResponseWriter, e error) {
	if errors.Is(e, errForbidden) {
		fail(w, 403, e.Error())
	} else {
		fail(w, 404, "Resource not found")
	}
}
