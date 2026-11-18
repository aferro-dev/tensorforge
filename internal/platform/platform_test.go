package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCalculator(t *testing.T) {
	for _, tc := range []struct {
		expression string
		value      float64
		bad        bool
	}{{"(2.4-1.8)/2.4*100", 25, false}, {"2+3*4", 14, false}, {"1/0", 0, true}, {"os.Exit(1)", 0, true}, {"a+1", 0, true}, {strings.Repeat("(", 40) + "1" + strings.Repeat(")", 40), 0, true}} {
		v, e := calculate(tc.expression)
		if tc.bad {
			if e == nil {
				t.Errorf("accepted %s", tc.expression)
			}
		} else if e != nil || v < tc.value-.0001 || v > tc.value+.0001 {
			t.Errorf("%s = %v, %v", tc.expression, v, e)
		}
	}
}
func TestValidation(t *testing.T) {
	if _, issues := validateDataset([]byte("{\"text\":\"hello\"}\n{\"prompt\":\"x\",\"completion\":\"y\"}")); len(issues) != 0 {
		t.Fatal(issues)
	}
	for _, s := range []string{"", "{}", "oops", "{\"prompt\":\"x\"}"} {
		if _, issues := validateDataset([]byte(s)); len(issues) == 0 {
			t.Fatal("accepted", s)
		}
	}
	if e := validateCall(ToolCall{"calculator", map[string]any{"expression": "2+2", "shell": "whoami"}}); e == nil {
		t.Fatal("extra argument accepted")
	}
	if e := validateChat(ChatInput{Model: "x", Messages: []Message{{"user", "hello"}}, MaxTokens: 100, Temperature: 0}); e != nil {
		t.Fatal(e)
	}
}
func TestAdmission(t *testing.T) {
	a := &App{cfg: Config{QueueTimeout: 20 * time.Millisecond}, slots: make(chan struct{}, 1), waiting: make(chan struct{}, 2)}
	_, release, e := a.acquire(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, e = a.acquire(ctx); e == nil {
		t.Fatal("cancelled waiter accepted")
	}
	if len(a.waiting) != 1 {
		t.Fatal("leaked queue slot")
	}
	if _, _, e = a.acquire(context.Background()); e == nil {
		t.Fatal("queue timeout ignored")
	}
}
func TestIntegration(t *testing.T) {
	uri := os.Getenv("TF_TEST_MONGODB_URI")
	redisURL := os.Getenv("TF_TEST_REDIS_URL")
	if uri == "" || redisURL == "" {
		t.Skip("set TF_TEST_MONGODB_URI and TF_TEST_REDIS_URL (with Redis Search) for integration tests")
	}
	t.Setenv("TF_WEB_DIR", "../../web")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var activeModel string
	var modelMu sync.Mutex
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+strings.Repeat("s", 64) {
			t.Error("missing internal auth")
		}
		switch r.URL.Path {
		case "/reset":
			respond(w, 200, map[string]any{"status": "stopped"})
		case "/gpu":
			respond(w, 200, map[string]any{"available": false, "error": "test fixture has no GPU"})
		case "/embed":
			v := make([]float64, 384)
			v[0] = 1
			respond(w, 200, map[string]any{"vectors": [][]float64{v}, "embedding_id": "fixture@immutable"})
		case "/operations":
			var in map[string]any
			json.NewDecoder(r.Body).Decode(&in)
			p, _ := in["payload"].(map[string]any)
			if in["operation"] == "model_start" {
				modelMu.Lock()
				activeModel = str(p["model"])
				modelMu.Unlock()
			}
			respond(w, 200, map[string]any{"fixture": true})
		case "/chat":
			var in map[string]any
			json.NewDecoder(r.Body).Decode(&in)
			modelMu.Lock()
			matches := in["model"] == activeModel
			modelMu.Unlock()
			if !matches {
				t.Error("wrong model")
			}
			if in["stream"] == true {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\ndata: {\"usage\":{\"completion_tokens\":1},\"choices\":[]}\n\ndata: [DONE]\n\n")
			} else {
				respond(w, 200, map[string]any{"id": "chatcmpl-fixture", "object": "chat.completion", "choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "Grounded answer [1]"}, "finish_reason": "stop"}}, "usage": map[string]any{"completion_tokens": 4}})
			}
		default:
			respond(w, 200, map[string]any{})
		}
	}))
	defer ai.Close()
	isolation := "tensorforge_test_" + token()[:12]
	cfg := Config{MongoURI:uri,MongoDatabase:isolation,RedisURL:redisURL,RedisNamespace:isolation, AdminKey: strings.Repeat("a", 64), ServiceKey: strings.Repeat("s", 64), AIURL: ai.URL, Concurrency: 1, QueueSize: 2, QueueTimeout: time.Second, JobTimeout: time.Minute, WriteApproval: true}
	a, e := New(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		a.shutdown()
		a.workerDone.Wait()
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		a.store.DB.Drop(cleanup)
		a.vectors.Client.Do(cleanup,"FT.DROPINDEX",a.vectors.Index,"DD")
		a.Close()
	}()
	a.Run(ctx)
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	call := func(method, path, key string, body any) (int, map[string]any, string) {
		t.Helper()
		var reader io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			reader = bytes.NewReader(b)
		}
		req, _ := http.NewRequest(method, srv.URL+path, reader)
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		req.Header.Set("Content-Type", "application/json")
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		var out map[string]any
		json.Unmarshal(b, &out)
		return res.StatusCode, out, string(b)
	}
	if code, _, _ := call("GET", "/api/me", "", nil); code != 401 {
		t.Fatal("unauthenticated", code)
	}
	_, dev, _ := call("POST", "/api/keys", cfg.AdminKey, map[string]any{"name": "Developer", "role": "developer"})
	devKey := str(dev["key"])
	devID := str(dev["id"])
	if devKey == "" {
		t.Fatal(dev)
	}
	_, viewer, _ := call("POST", "/api/keys", cfg.AdminKey, map[string]any{"name": "Viewer", "role": "viewer"})
	viewerKey := str(viewer["key"])
	if code, _, _ := call("POST", "/api/models", viewerKey, map[string]any{}); code != 403 {
		t.Fatal("viewer write allowed", code)
	}
	modelInput := map[string]any{"model_id": "example/tiny", "revision": strings.Repeat("a", 40), "license": "apache-2.0", "estimated_vram_mb": 1000}
	code, model, body := call("POST", "/api/models", cfg.AdminKey, modelInput)
	if code != 201 {
		t.Fatal(code, body)
	}
	modelID := str(model["id"])
	code, job, _ := call("POST", "/api/models/"+modelID+"/start", cfg.AdminKey, nil)
	if code != 202 {
		t.Fatal(code, job)
	}
	waitJob(t, a, str(job["id"]), "succeeded")
	code, completion, _ := call("POST", "/v1/chat/completions", devKey, map[string]any{"model": modelID, "messages": []Message{{"user", "hi"}}})
	if code != 200 || completion["object"] != "chat.completion" {
		t.Fatal(code, completion)
	}
	code, _, body = call("POST", "/v1/chat/completions", devKey, map[string]any{"model": modelID, "messages": []Message{{"user", "hi"}}, "stream": true})
	if code != 200 || !strings.Contains(body, "[DONE]") {
		t.Fatal(code, body)
	}
	// Private documents: MongoDB ACLs restrict Redis KNN before ranking/LIMIT.
	vec := make([]any, 384)
	for i := range vec {
		vec[i] = float64(0)
	}
	vec[0] = float64(1)
	for i, owner := range []string{devID, "other-user"} {
		doc := Record{ID: fmt.Sprintf("doc-%d", i), Kind: "document", Owner: owner, Status: "ready", Data: map[string]any{"name": fmt.Sprintf("source-%d", i), "readers": []string{}}}
		if e = a.store.Put(ctx, doc); e != nil {
			t.Fatal(e)
		}
		if e = a.saveChunks(ctx, doc, map[string]any{"embedding_id": "fixture@immutable", "chunks": []any{map[string]any{"text": owner, "embedding": vec}}}); e != nil {
			t.Fatal(e)
		}
		if e = a.store.Put(ctx, doc); e != nil { t.Fatal(e) }
	}
	code, out, _ := call("POST", "/api/retrieval/search", devKey, map[string]any{"query": "test", "top_k": 4})
	if code != 200 || len(array(out["passages"])) != 1 {
		t.Fatal("ACL leak", code, out)
	}
	_, reader, _ := call("POST", "/api/keys", cfg.AdminKey, map[string]any{"name":"Reader","role":"developer"})
	readerKey, readerID := str(reader["key"]), str(reader["id"])
	code, out, _ = call("POST", "/api/retrieval/search", readerKey, map[string]any{"query":"test","top_k":4})
	if code!=200 || len(array(out["passages"]))!=0 { t.Fatal("private document exposed",code,out) }
	code, _, _ = call("PUT", "/api/documents/doc-0/acl", devKey, map[string]any{"readers":[]string{readerID}})
	if code!=200 { t.Fatal("share document",code) }
	code, out, _ = call("POST", "/api/retrieval/search", readerKey, map[string]any{"query":"test","top_k":4})
	if code!=200 || len(array(out["passages"]))!=1 { t.Fatal("shared document missing",code,out) }
	call("PUT", "/api/documents/doc-0/acl", devKey, map[string]any{"readers":[]string{}})
	code, out, _ = call("POST", "/api/retrieval/search", readerKey, map[string]any{"query":"test","top_k":4})
	if code!=200 || len(array(out["passages"]))!=0 { t.Fatal("revoked ACL still permits Redis results",code,out) }
	code, out, _ = call("POST", "/api/retrieval/answer", devKey, map[string]any{"query": "test", "model": modelID})
	if code != 200 || len(array(out["citations"])) != 1 {
		t.Fatal("grounded answer", code, out)
	}
	if code, _, _ := call("PUT", "/api/documents/doc-0/acl", viewerKey, map[string]any{"readers": []string{str(viewer["id"])}}); code != 403 {
		t.Fatal("viewer ACL mutation", code)
	}
	_, agent, _ := call("POST", "/api/agents", devKey, map[string]any{"name": "bounded", "tools": []string{"calculator", "write_note"}})
	_, run, _ := call("POST", "/api/agents/"+str(agent["id"])+"/runs", devKey, map[string]any{"calls": []ToolCall{{"write_note", map[string]any{"title": "review", "content": "needs approval"}}}})
	if run["status"] != "awaiting_approval" {
		t.Fatal(run)
	}
	notes, err := a.store.Count(ctx,"note")
	if err != nil { t.Fatal(err) }
	if notes != 0 {
		t.Fatal("write ran before approval")
	}
	_, rejected, _ := call("POST", "/api/runs/"+str(run["id"])+"/approval", cfg.AdminKey, map[string]bool{"approve": false})
	if rejected["status"] != "rejected" {
		t.Fatal(rejected)
	}
	notes, err = a.store.Count(ctx,"note")
	if err != nil { t.Fatal(err) }
	if notes != 0 {
		t.Fatal("rejected action executed")
	}
	_, run, _ = call("POST", "/api/agents/"+str(agent["id"])+"/runs", devKey, map[string]any{"calls": []ToolCall{{"write_note", map[string]any{"title": "approved", "content": "yes"}}}})
	code, out, _ = call("POST", "/api/runs/"+str(run["id"])+"/approval", cfg.AdminKey, map[string]bool{"approve": true})
	if code != 200 || out["status"] != "succeeded" {
		t.Fatal(code, out)
	}
	if code, _, _ := call("POST", "/api/runs/"+str(run["id"])+"/approval", cfg.AdminKey, map[string]bool{"approve": true}); code != 409 {
		t.Fatal("duplicate approval", code)
	}
	firstAudit, err := a.store.AuditList(ctx)
	if err!=nil { t.Fatal(err) }
	secondAudit, err := a.store.AuditList(ctx)
	if err!=nil { t.Fatal(err) }
	if len(firstAudit)!=len(secondAudit) { t.Fatal("audit outbox duplicated events") }
	if len(firstAudit)==0 { t.Fatal("audit missing") }
	code, _, body = call("GET", "/ui", devKey, nil)
	if code != 200 || !strings.Contains(body, "hx-get") || !strings.Contains(body, "TensorForge") {
		t.Fatal("SSR", code, body)
	}
	req, _ := http.NewRequest("POST", srv.URL+"/api/agents", strings.NewReader(`{"name":"csrf","tools":["calculator"]}`))
	req.AddCookie(&http.Cookie{Name: "tf_session", Value: devKey})
	res, _ := http.DefaultClient.Do(req)
	res.Body.Close()
	if res.StatusCode != 403 {
		t.Fatal("CSRF accepted", res.StatusCode)
	}
	// Upload validation reports exact invalid line; successful dataset retains content hash.
	var upload bytes.Buffer
	mw := multipart.NewWriter(&upload)
	part, _ := mw.CreateFormFile("file", "bad.jsonl")
	part.Write([]byte("{}\n"))
	mw.Close()
	req, _ = http.NewRequest("POST", srv.URL+"/api/datasets", &upload)
	req.Header.Set("Authorization", "Bearer "+devKey)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	res, _ = http.DefaultClient.Do(req)
	res.Body.Close()
	if res.StatusCode != 422 {
		t.Fatal("invalid dataset", res.StatusCode)
	}
	if code, _, _ := call("DELETE", "/api/keys/"+devID, cfg.AdminKey, nil); code != 200 {
		t.Fatal("revoke", code)
	}
	if code, _, _ := call("GET", "/api/me", devKey, nil); code != 401 {
		t.Fatal("revocation ignored", code)
	}
	// Durable recovery preserves prior history and clearly marks interrupted executions.
	a.store.Put(ctx, Record{ID: "interrupted", Kind: "job", Owner: "bootstrap-admin", Status: "running", Data: map[string]any{"operation": "benchmark"}})
	if e = a.store.Recover(ctx); e != nil {
		t.Fatal(e)
	}
	row, _ := a.store.Get(ctx, "interrupted")
	if row.Status != "interrupted" || row.Error == "" {
		t.Fatal(row)
	}
}
func waitJob(t *testing.T, a *App, id, status string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		row, e := a.store.Get(context.Background(), id)
		if e == nil && row.Status == status {
			return
		}
		if row.Status == "failed" {
			t.Fatal(row.Error)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job timeout", id)
}

func TestPublicJobOmitsSourceWithoutMutatingStorage(t *testing.T) {
 original := Record{Kind:"job",Data:map[string]any{"operation":"train","payload":map[string]any{"records":[]any{"private training data"},"dataset_version":"sha"},"readers":[]string{"viewer"}}}
 visible := publicRecord(original)
 payload := visible.Data["payload"].(map[string]any)
 if _, exposed := payload["records"]; exposed { t.Fatal("training corpus exposed") }
 if payload["dataset_version"] != "sha" { t.Fatal("lost reproducibility metadata") }
 stored := original.Data["payload"].(map[string]any)
 if _, preserved := stored["records"]; !preserved { t.Fatal("mutated durable manifest") }
 if !readable(original,User{ID:"viewer",Role:"viewer"}) { t.Fatal("shared report unreadable") }
 if readable(original,User{ID:"stranger",Role:"viewer"}) { t.Fatal("private report exposed") }
}
