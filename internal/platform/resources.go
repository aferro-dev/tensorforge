package platform

import (
	"encoding/json"
	"net/http"
	"regexp"
	"time"
)

func (a *App) keys(w http.ResponseWriter, r *http.Request) {
	if !require(w, r, "administrator") {
		return
	}
	if r.Method == "GET" {
		rows, e := a.store.Keys(r.Context())
		if e != nil {
			fail(w, 500, e.Error())
			return
		}
		respond(w, 200, rows)
		return
	}
	var in struct {
		Name string `json:"name"`
		Role string `json:"role"`
	}
	if !decode(w, r, &in) {
		return
	}
	if len(in.Name) < 1 || len(in.Name) > 100 || (in.Role != "administrator" && in.Role != "developer" && in.Role != "viewer") {
		fail(w, 422, "Name and valid role required")
		return
	}
	key := token()
	id := token()[:24]
	e := a.store.CreateKey(r.Context(), current(r).ID, KeyRecord{ID:id,Digest:hash(key),Name:in.Name,Role:in.Role})
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	respond(w, 201, map[string]any{"id": id, "key": key, "role": in.Role, "notice": "Store this key now; it is never shown again"})
}
func (a *App) revokeKey(w http.ResponseWriter, r *http.Request) {
	if !require(w, r, "administrator") {
		return
	}
	e := a.store.RevokeKey(r.Context(), r.PathValue("id"), current(r).ID)
	if e != nil {
		if e == errNotFound { fail(w, 404, "Key not found"); return }
		fail(w, 500, e.Error())
		return
	}
	respond(w, 200, map[string]bool{"revoked": true})
}

type ModelInput struct {
	ModelID       string         `json:"model_id"`
	Revision      string         `json:"revision"`
	License       string         `json:"license"`
	Configuration map[string]any `json:"configuration"`
	EstimatedVRAM int            `json:"estimated_vram_mb"`
	MaxModelLen   int            `json:"max_model_len"`
	Dtype         string         `json:"dtype"`
	Quantization  string         `json:"quantization,omitempty"`
}

var repoPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
var shaPattern = regexp.MustCompile(`^[a-fA-F0-9]{40}$`)

func (a *App) models(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		a.list(w, r, "model")
		return
	}
	if !require(w, r, "administrator") {
		return
	}
	var in ModelInput
	if !decode(w, r, &in) {
		return
	}
	if in.MaxModelLen == 0 {
		in.MaxModelLen = 2048
	}
	if in.Dtype == "" {
		in.Dtype = "half"
	}
	if !repoPattern.MatchString(in.ModelID) || !shaPattern.MatchString(in.Revision) || len(in.License) < 1 || len(in.License) > 200 || in.EstimatedVRAM < 256 || in.EstimatedVRAM > 7000 || in.MaxModelLen < 128 || in.MaxModelLen > 8192 || (in.Dtype != "half" && in.Dtype != "auto" && in.Dtype != "bfloat16") || (in.Quantization != "" && in.Quantization != "awq" && in.Quantization != "gptq" && in.Quantization != "bitsandbytes") {
		fail(w, 422, "Valid repository, immutable 40-character revision, license, dtype and VRAM estimate (256–7000 MiB) required")
		return
	}
	b, _ := json.Marshal(in)
	var data map[string]any
	json.Unmarshal(b, &data)
	row := Record{ID: token()[:24], Kind: "model", Owner: current(r).ID, Status: "registered", Data: data}
	if e := a.store.Put(r.Context(), row); e != nil {
		fail(w, 500, e.Error())
		return
	}
	a.store.Audit(r.Context(), current(r).ID, "model.register", row.ID, data)
	respond(w, 201, row)
}
func (a *App) modelAction(w http.ResponseWriter, r *http.Request) {
	if !require(w, r, "administrator") {
		return
	}
	row, e := a.store.Access(r.Context(), r.PathValue("id"), "model", current(r), true)
	if e != nil {
		accessError(w, e)
		return
	}
	action := r.PathValue("action")
	if action != "download" && action != "start" && action != "stop" {
		fail(w, 404, "Unknown action")
		return
	}
	job, e := a.enqueue(r.Context(), current(r), "model_"+action, map[string]any{"model": row.ID, "configuration": row.Data})
	if e != nil {
		fail(w, 409, e.Error())
		return
	}
	respond(w, 202, publicRecord(job))
}
func (a *App) gpu(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := timeout(r.Context(), 5*time.Second)
	defer cancel()
	out, e := a.ai(ctx, "/gpu", nil)
	if e != nil {
		respond(w, 200, map[string]any{"available": false, "error": e.Error()})
		return
	}
	respond(w, 200, out)
}
func (a *App) openaiModels(w http.ResponseWriter, r *http.Request) {
	rows, e := a.store.List(r.Context(), "model")
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	out := []map[string]any{}
	for _, v := range rows {
		if v.Status == "running" {
			out = append(out, map[string]any{"id": v.ID, "object": "model", "owned_by": v.Data["model_id"]})
		}
	}
	respond(w, 200, map[string]any{"object": "list", "data": out})
}
func (a *App) auditList(w http.ResponseWriter, r *http.Request) {
	if !require(w, r, "administrator") {
		return
	}
	rows, e := a.store.AuditList(r.Context())
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	respond(w, 200, rows)
}
