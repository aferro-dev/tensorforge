package platform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Templates struct{ t *template.Template }

func loadTemplates() (*Templates, error) {
	t, e := template.New("").Funcs(template.FuncMap{"pretty": func(v any) string {
		b, _ := json.MarshalIndent(v, "", "  ")
		if len(b) > 18000 {
			return string(b[:18000]) + "\n… (full record available through API)"
		}
		return string(b)
	}, "title": strings.Title}).ParseGlob(env("TF_WEB_DIR", "web") + "/templates/*.html")
	return &Templates{t}, e
}

type Field struct {
	Name, Label, Type, Value, Placeholder string
	Required                              bool
}
type Form struct {
	Action, Title, Description string
	Fields                     []Field
}

func field(n, l, t, v string) Field {
	return Field{Name: n, Label: l, Type: t, Value: v, Required: true}
}

var forms = map[string][]Form{
	"models":      {{"model", "Register model", "Use a verified Hugging Face commit SHA. VRAM estimate is a conservative admission check, not a guarantee.", []Field{field("model_id", "Hugging Face repository", "text", ""), field("revision", "Immutable revision (40-character SHA)", "text", ""), field("license", "License", "text", ""), field("estimated_vram_mb", "Estimated peak VRAM (MiB)", "number", "4000"), field("max_model_len", "Maximum context tokens", "number", "2048"), field("dtype", "Dtype: half / auto / bfloat16", "text", "half"), {Name: "quantization", Label: "Quantization (optional): awq / gptq / bitsandbytes", Type: "text"}}}},
	"documents":   {{"document", "Ingest document", "Text, Markdown or PDF; 10 MiB maximum. Documents are private until shared.", []Field{field("file", "Document", "file", "")}}, {"acl", "Share document", "Only owner or administrator can change readers. Use API key IDs.", []Field{field("id", "Document ID", "text", ""), field("readers", "Reader IDs (JSON array)", "textarea", "[]")}}},
	"retrieval":   {{"search", "Search knowledge", "Only authorized chunks enter retrieval results.", []Field{field("query", "Question", "textarea", ""), field("top_k", "Passages", "number", "4")}}, {"answer", "Grounded answer", "A running model is required. Unknown or missing citations cause the answer to be withheld.", []Field{field("model", "Running model ID", "text", ""), field("query", "Question", "textarea", ""), field("top_k", "Passages", "number", "4"), field("max_tokens", "Output tokens", "number", "256")}}},
	"inference":   {{"chat", "Chat completion", "Bounded admission and request cancellation. Streaming is available through the OpenAI-compatible API.", []Field{field("model", "Running model ID", "text", ""), field("prompt", "Message", "textarea", ""), field("max_tokens", "Output tokens", "number", "256")}}},
	"datasets":    {{"dataset", "Upload JSONL dataset", "Each line contains text or prompt/completion. Evaluation requires prompt/completion pairs.", []Field{field("file", "JSONL dataset", "file", "")}}},
	"training":    {{"train", "LoRA training", "Stop inference first: GPU workloads are exclusive. Requires the GPU image and CUDA.", []Field{field("model", "Registered model ID", "text", ""), field("dataset", "Dataset ID", "text", ""), field("seed", "Seed", "number", "42"), field("steps", "Optimizer steps", "number", "20"), field("learning_rate", "Learning rate", "text", "0.0002"), field("rank", "LoRA rank", "number", "8"), field("max_length", "Sequence length", "number", "256"), field("checkpoint_steps", "Checkpoint interval", "number", "10"), {Name: "resume_job", Label: "Resume job ID (optional)", Type: "text"}}}},
	"evaluations": {{"evaluate", "Compare baseline and adapter", "Runs identical prompts and seeds; persists exact match, token F1 and complete predictions.", []Field{field("model", "Registered model ID", "text", ""), field("dataset", "Evaluation dataset ID", "text", ""), field("adapter_job", "Successful training job ID", "text", ""), field("seed", "Seed", "number", "42"), field("max_new_tokens", "Output tokens", "number", "64")}}},
	"benchmarks":  {{"benchmark", "PyTorch vs Triton vector addition", "CUDA events, correctness checks, raw samples and environment metadata. No prefilled performance claims.", []Field{field("size", "Elements", "number", "1048576"), field("dtype", "float16 / float32", "text", "float32"), field("warmup", "Warmup iterations", "number", "25"), field("repetitions", "Measured repetitions", "number", "100"), field("seed", "Seed", "number", "42")}}},
	"jobs": {{"job-acl", "Share experiment results", "Share job metadata, reports and artifacts with API key IDs, including viewers. Uploaded source corpora and internal manifests remain private.", []Field{field("id", "Job ID", "text", ""), field("readers", "Reader IDs (JSON array)", "textarea", "[]")}}},
	"agents":      {{"agent", "Configure bounded agent", "Tools execute as an explicit bounded plan. No shell, arbitrary code or external network tools.", []Field{field("name", "Name", "text", ""), field("tools", "Approved tools (JSON)", "textarea", `["calculator","search","write_note"]`)}}, {"run", "Execute plan", "Writes pause for administrator approval. Maximum eight tool calls.", []Field{field("id", "Agent ID", "text", ""), field("calls", "Tool calls (JSON)", "textarea", `[{"tool":"calculator","arguments":{"expression":"(2.4-1.8)/2.4*100"}}]`)}}},
	"users":       {{"key", "Create API key", "The secret is returned only once. Users authenticate through individually revocable keys.", []Field{field("name", "Name", "text", ""), field("role", "administrator / developer / viewer", "text", "developer")}}},
}

type View struct {
	User    User
	CSRF    string
	Section string
	Forms   []Form
	Rows    []Record
	GPU     map[string]any
	Nav     []string
	Output  any
	Error   string
	Keys    []map[string]any
}

func (a *App) view(r *http.Request, section string) View {
	csrf := ""
	if c, e := r.Cookie("tf_session"); e == nil {
		csrf = hash(c.Value + "csrf")
	}
	return View{User: current(r), CSRF: csrf, Section: section, Forms: forms[section], Nav: []string{"overview", "models", "inference", "documents", "retrieval", "agents", "runs", "datasets", "training", "evaluations", "benchmarks", "jobs", "users", "audit", "requests"}}
}
func (a *App) render(w http.ResponseWriter, name string, v any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if e := a.templates.t.ExecuteTemplate(w, name, v); e != nil {
		fmt.Fprint(w, "Template rendering failed")
	}
}
func (a *App) login(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		a.render(w, "login", View{})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	key := r.FormValue("key")
	if len(key) < 32 { w.WriteHeader(401); a.render(w, "login", View{Error: "Invalid API key"}); return }
	clone := r.Clone(r.Context())
	clone.Header = r.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+key)
	if _, e := a.authenticate(clone); e != nil {
		w.WriteHeader(401)
		a.render(w, "login", View{Error: "Invalid API key"})
		return
	}
	http.SetCookie(w, &http.Cookie{Name: "tf_session", Value: key, Path: "/", HttpOnly: true, Secure: a.cfg.SecureCookies, SameSite: http.SameSiteStrictMode, MaxAge: 28800})
	http.Redirect(w, r, "/ui", 303)
}
func (a *App) logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: "tf_session", Path: "/", HttpOnly: true, Secure: a.cfg.SecureCookies, SameSite: http.SameSiteStrictMode, MaxAge: -1})
	w.Header().Set("HX-Redirect", "/login")
	http.Redirect(w, r, "/login", 303)
}
func (a *App) dashboard(w http.ResponseWriter, r *http.Request) {
	section := r.URL.Query().Get("section")
	if !validSection(section) { section = "overview" }
	v := a.view(r, section)
	a.fillView(r, &v)
	a.render(w, "layout", v)
}
func (a *App) panel(w http.ResponseWriter, r *http.Request) {
	if !validSection(r.PathValue("kind")) { http.NotFound(w, r); return }
	v := a.view(r, r.PathValue("kind"))
	a.fillView(r, &v)
	a.render(w, "panel", v)
}
func (a *App) fillView(r *http.Request, v *View) {
	kind := map[string]string{"models": "model", "documents": "document", "agents": "agent", "runs": "run", "datasets": "dataset", "jobs": "job", "training": "job", "evaluations": "job", "benchmarks": "job"}[v.Section]
	if kind != "" {
		rows, e := a.store.List(r.Context(), kind)
		if e != nil {
			v.Error = e.Error()
			return
		}
		for _, row := range rows {
			if !readable(row, v.User) {
				continue
			}
			op := str(row.Data["operation"])
			if v.Section == "training" && op != "train" {
				continue
			}
			if v.Section == "evaluations" && op != "evaluate" {
				continue
			}
			if v.Section == "benchmarks" && op != "benchmark" {
				continue
			}
			v.Rows = append(v.Rows, publicRecord(row))
		}
	}
	if v.Section == "overview" {
		ctx, cancel := timeout(r.Context(), 3*time.Second)
		defer cancel()
		v.GPU, _ = a.ai(ctx, "/gpu", nil)
		if v.GPU == nil {
			v.GPU = map[string]any{"available": false, "reason": "AI service unreachable"}
		}
		rows, _ := a.store.List(r.Context(), "job")
		for _, row := range rows {
			if readable(row, v.User) && len(v.Rows) < 8 {
				v.Rows = append(v.Rows, publicRecord(row))
			}
		}
	}
	if v.Section == "users" || v.Section == "audit" || v.Section == "requests" {
		if v.User.Role != "administrator" {
			v.Error = "Administrator access required"
			v.Forms = nil
			return
		}
		path := map[string]string{"users": "/api/keys", "audit": "/api/audit", "requests": "/api/requests"}[v.Section]
		req := r.Clone(r.Context())
		req.Method = "GET"
		req.URL = &url.URL{Path: path}
		c := &captureWriter{header: http.Header{}}
		switch v.Section {
		case "users":
			a.keys(c, req)
		case "audit":
			a.auditList(c, req)
		case "requests":
			a.requests(c, req)
		}
		json.Unmarshal(c.body, &v.Output)
	}
}
func (a *App) uiAction(w http.ResponseWriter, r *http.Request) {
	action := r.PathValue("action")
	if action == "document" || action == "dataset" {
		req := r.Clone(r.Context())
		c := &captureWriter{header: http.Header{}}
		if action == "document" {
			a.documents(c, req)
		} else {
			a.datasets(c, req)
		}
		a.renderResponse(w, r, c)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 11<<20)
	if e := r.ParseForm(); e != nil {
		a.render(w, "result", View{Error: e.Error()})
		return
	}
	data := map[string]any{}
	for k, v := range r.PostForm {
		if k != "csrf" && len(v) > 0 && v[0] != "" {
			data[k] = v[0]
		}
	}
	id := str(data["id"])
	delete(data, "id")
	path := ""
	method := "POST"
	switch action {
	case "model":
		path = "/api/models"
	case "start", "stop", "download":
		path = "/api/models/" + url.PathEscape(id) + "/" + action
	case "cancel":
		path = "/api/jobs/" + url.PathEscape(id) + "/cancel"
	case "key":
		path = "/api/keys"
	case "revoke":
		path = "/api/keys/" + url.PathEscape(id)
		method = "DELETE"
	case "agent":
		path = "/api/agents"
	case "run":
		path = "/api/agents/" + url.PathEscape(id) + "/runs"
	case "approve", "reject":
		path = "/api/runs/" + url.PathEscape(id) + "/approval"
		data["approve"] = action == "approve"
	case "acl":
		path = "/api/documents/" + url.PathEscape(id) + "/acl"
		method = "PUT"
	case "reindex":
		path = "/api/documents/" + url.PathEscape(id) + "/reindex"
	case "job-acl":
		path = "/api/jobs/" + url.PathEscape(id) + "/acl"
		method = "PUT"
	case "search":
		path = "/api/retrieval/search"
	case "answer":
		path = "/api/retrieval/answer"
	case "chat":
		path = "/v1/chat/completions"
		data["messages"] = []Message{{Role: "user", Content: str(data["prompt"])}}
		delete(data, "prompt")
	case "train":
		path = "/api/training"
	case "evaluate":
		path = "/api/evaluations"
	case "benchmark":
		path = "/api/benchmarks"
	default:
		a.render(w, "result", View{Error: "Unknown action"})
		return
	}
	for _, key := range []string{"estimated_vram_mb", "max_model_len", "top_k", "max_tokens", "seed", "steps", "learning_rate", "rank", "max_length", "checkpoint_steps", "max_new_tokens", "size", "warmup", "repetitions"} {
		if s, ok := data[key].(string); ok {
			var n json.Number
			if json.Unmarshal([]byte(s), &n) != nil {
				a.render(w, "result", View{Error: "Invalid number: " + key})
				return
			}
			data[key] = n
		}
	}
	for _, key := range []string{"tools", "calls", "readers"} {
		if s, ok := data[key].(string); ok {
			var value any
			if json.Unmarshal([]byte(s), &value) != nil {
				a.render(w, "result", View{Error: "Invalid JSON: " + key})
				return
			}
			data[key] = value
		}
	}
	body, _ := json.Marshal(data)
	req := r.Clone(r.Context())
	req.Method = method
	req.URL = &url.URL{Path: path}
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.Header = r.Header.Clone()
	req.Header.Set("Content-Type", "application/json")
	if r.FormValue("csrf") != "" {
		req.Header.Set("X-CSRF-Token", r.FormValue("csrf"))
	}
	req.Header.Set("Authorization", r.Header.Get("Authorization"))
	req.Form = nil
	req.PostForm = nil
	req.MultipartForm = nil
	c := &captureWriter{header: http.Header{}}
	a.Handler().ServeHTTP(c, req)
	a.renderResponse(w, r, c)
}
func (a *App) renderResponse(w http.ResponseWriter, r *http.Request, c *captureWriter) {
	v := a.view(r, "")
	if c.status >= 400 {
		v.Error = string(c.body)
	} else {
		json.Unmarshal(c.body, &v.Output)
	}
	if r.Header.Get("HX-Request") == "true" {
		a.render(w, "result", v)
	} else {
		v.Section = "overview"
		a.render(w, "layout", v)
	}
}

func validSection(section string) bool {
	for _, name := range []string{"overview", "models", "inference", "documents", "retrieval", "agents", "runs", "datasets", "training", "evaluations", "benchmarks", "jobs", "users", "audit", "requests"} {
		if section == name { return true }
	}
	return false
}

func (a *App) jobProgress(w http.ResponseWriter, r *http.Request) {
	row, err := a.store.Access(r.Context(), r.PathValue("id"), "job", current(r), false)
	if err != nil { http.NotFound(w, r); return }
	ctx, cancel := timeout(r.Context(), 2*time.Second)
	defer cancel()
	progress, err := a.ai(ctx, "/progress/"+row.ID, nil)
	if err != nil { progress = map[string]any{"status": row.Status, "message": "Live worker progress unavailable"} }
	a.render(w, "progress", progress)
}
