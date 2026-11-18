package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

func timeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}
func (a *App) enqueue(ctx context.Context, u User, operation string, payload map[string]any) (Record, error) {
	// Serializes queue capacity checks across concurrent requests.
	a.mu.Lock()
	defer a.mu.Unlock()
	count, e := a.store.PendingJobs(ctx)
	if e != nil {
		return Record{}, e
	}
	if count >= 64 {
		return Record{}, errors.New("Job queue full (64)")
	}
	row := Record{ID: token()[:24], Kind: "job", Owner: u.ID, Status: "queued", Data: map[string]any{"operation": operation, "payload": payload, "events": []any{}}}
	if e := a.store.PutAudited(ctx, row, u.ID, "job.enqueue", map[string]any{"operation": operation}); e != nil {
		return row, e
	}
	select {
	case a.wake <- struct{}{}:
	default:
	}
	return row, nil
}
func (a *App) jobs(w http.ResponseWriter, r *http.Request) { a.list(w, r, "job") }
func (a *App) job(w http.ResponseWriter, r *http.Request) {
	row, e := a.store.Access(r.Context(), r.PathValue("id"), "job", current(r), false)
	if e != nil {
		accessError(w, e)
		return
	}
	if row.Status == "running" || row.Status == "failed" || row.Status == "cancelled" || row.Status == "interrupted" {
		ctx, done := timeout(r.Context(), 2*time.Second)
		if progress, err := a.ai(ctx, "/progress/"+row.ID, nil); err == nil {
			row.Data["progress"] = progress
		}
		done()
	}
	respond(w, 200, publicRecord(row))
}
func (a *App) cancelJob(w http.ResponseWriter, r *http.Request) {
	if !writeAccess(w, r) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	row, e := a.store.Access(r.Context(), r.PathValue("id"), "job", current(r), true)
	if e != nil {
		accessError(w, e)
		return
	}
	if row.Status != "running" && row.Status != "queued" {
		fail(w, 409, "Job is already terminal")
		return
	}
	wasQueued := row.Status == "queued"
	row.Status = "cancelled"
	row.Error = "Cancelled by user"
	if e = a.store.Put(r.Context(), row); e != nil {
		fail(w, 500, e.Error())
		return
	}
	if cancel := a.cancel[row.ID]; cancel != nil {
		cancel()
	}
	if wasQueued && str(row.Data["operation"]) == "ingest" {
		payload, _ := row.Data["payload"].(map[string]any)
		if doc, err := a.store.Get(r.Context(), str(payload["document"])); err == nil {
			doc.Status = "failed"
			doc.Error = "Ingestion cancelled before execution"
			a.store.Put(r.Context(), doc)
		}
	}
	a.store.Audit(r.Context(), current(r).ID, "job.cancel", row.ID, nil)
	respond(w, 200, publicRecord(row))
}
func (a *App) worker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.wake:
		case <-ticker.C:
		}
		for ctx.Err() == nil {
			job, ok := a.claimJob(ctx)
			if !ok {
				break
			}
			a.executeJob(ctx, job)
		}
	}
}
func (a *App) claimJob(ctx context.Context) (Record, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	row, e := a.store.ClaimJob(ctx)
	if e != nil {
		return row, false
	}
	return row, true
}
func (a *App) executeJob(parent context.Context, job Record) {
	ctx, cancel := context.WithTimeout(parent, a.cfg.JobTimeout)
	a.mu.Lock()
	freshJob, lookupErr := a.store.Get(parent, job.ID)
	if lookupErr != nil || freshJob.Status != "running" {
		a.mu.Unlock()
		cancel()
		return
	}
	a.cancel[job.ID] = cancel
	a.mu.Unlock()
	defer func() { cancel(); a.mu.Lock(); delete(a.cancel, job.ID); a.mu.Unlock() }()
	operation := str(job.Data["operation"])
	payload, _ := job.Data["payload"].(map[string]any)
	if payload == nil {
		payload = map[string]any{}
	}
	if len(operation) > 6 && operation[:6] == "model_" {
		if m, err := a.store.Get(ctx, str(payload["model"])); err == nil {
			switch operation {
			case "model_start":
				m.Status = "starting"
			case "model_stop":
				m.Status = "stopping"
			case "model_download":
				m.Status = "downloading"
			}
			a.store.Put(ctx, m)
		}
	}
	input := map[string]any{"id": job.ID, "operation": operation, "payload": payload}
	result, e := a.ai(ctx, "/operations", input)
	// Persist through an independent context even if the caller cancelled or server is shutting down.
	saveCtx, saveCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer saveCancel()
	a.mu.Lock()
	defer a.mu.Unlock()
	fresh, getErr := a.store.Get(saveCtx, job.ID)
	if getErr != nil {
		return
	}
	wasCancelled := fresh.Status == "cancelled"
	job = fresh
	if !wasCancelled {
		if e != nil {
			job.Status = "failed"
			job.Error = e.Error()
			if parent.Err() != nil {
				job.Status = "interrupted"
			} else if ctx.Err() != nil {
				job.Status = "cancelled"
			}
		} else {
			job.Status = "succeeded"
			job.Data["result"] = result
		}
	}
	if operation == "ingest" {
		if result != nil {
			// Redis owns the vectors; never duplicate a dense result in a MongoDB job.
			job.Data["result"] = map[string]any{"embedding_id":result["embedding_id"],"sha256":result["sha256"],"chunk_count":len(array(result["chunks"]))}
		}
		doc, err := a.store.Get(saveCtx, str(payload["document"]))
		if err == nil {
			if e == nil && !wasCancelled {
				e = a.saveChunks(saveCtx, doc, result)
			}
			if e != nil || wasCancelled {
				doc.Status = "failed"
				doc.Error = "Ingestion failed or cancelled"
				if e != nil {
					doc.Error = e.Error()
					if !wasCancelled {
						job.Status = "failed"
						job.Error = e.Error()
					}
				}
			} else {
				doc.Status = "ready"
				doc.Data["embedding_id"] = result["embedding_id"]
				doc.Data["chunk_count"] = len(array(result["chunks"]))
			}
			if err := a.store.Put(saveCtx, doc); err != nil && !wasCancelled {
				job.Status = "failed"
				job.Error = "Cannot publish document metadata: " + err.Error()
			}
		}
	}
	if len(operation) > 6 && operation[:6] == "model_" {
		model, err := a.store.Get(saveCtx, str(payload["model"]))
		if err == nil {
			if e != nil || wasCancelled {
				model.Status = "failed"
				if operation == "model_stop" {
					model.Status = "unknown"
				}
				model.Error = job.Error
			} else {
				switch operation {
				case "model_download":
					model.Status = "downloaded"
					model.Data["download"] = result
				case "model_start":
					model.Status = "running"
				case "model_stop":
					model.Status = "stopped"
				}
				model.Error = ""
			}
			a.store.Put(saveCtx, model)
		}
	}
	a.store.Put(saveCtx, job)
	a.store.Audit(saveCtx, "worker", "job."+job.Status, job.ID, map[string]any{"operation": operation, "error": job.Error})
}
func (a *App) datasets(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		a.list(w, r, "dataset")
		return
	}
	if !writeAccess(w, r) {
		return
	}
	name, content, e := readUpload(w, r, "file")
	if e != nil {
		fail(w, 422, e.Error())
		return
	}
	records, issues := validateDataset(content)
	if len(issues) > 0 {
		respond(w, 422, map[string]any{"error": "Invalid dataset", "issues": issues})
		return
	}
	row := Record{ID: token()[:24], Kind: "dataset", Owner: current(r).ID, Status: "validated", Data: map[string]any{"name": name, "sha256": hash(string(content)), "records": records, "record_count": len(records)}}
	if e = a.store.Put(r.Context(), row); e != nil {
		fail(w, 500, e.Error())
		return
	}
	a.store.Audit(r.Context(), current(r).ID, "dataset.upload", row.ID, map[string]any{"sha256": row.Data["sha256"]})
	respond(w, 201, row)
}
func validateDataset(content []byte) ([]any, []string) {
	records := []any{}
	issues := []string{}
	for i, line := range splitLines(string(content)) {
		if len(line) == 0 {
			continue
		}
		var obj map[string]any
		if e := json.Unmarshal([]byte(line), &obj); e != nil {
			issues = append(issues, fmt.Sprintf("line %d: invalid JSON", i+1))
			continue
		}
		text := str(obj["text"])
		prompt := str(obj["prompt"])
		completion := str(obj["completion"])
		if (text == "" && (prompt == "" || completion == "")) || len(line) > 100000 {
			issues = append(issues, fmt.Sprintf("line %d: requires nonempty text or prompt/completion; max 100KB", i+1))
			continue
		}
		records = append(records, obj)
		if len(records) > 10000 {
			issues = append(issues, "Maximum 10000 records")
			break
		}
	}
	if len(records) == 0 {
		issues = append(issues, "Dataset is empty")
	}
	return records, issues
}

type TrainInput struct {
	Model           string  `json:"model"`
	Dataset         string  `json:"dataset"`
	Seed            int     `json:"seed"`
	Steps           int     `json:"steps"`
	LearningRate    float64 `json:"learning_rate"`
	Rank            int     `json:"rank"`
	MaxLength       int     `json:"max_length"`
	CheckpointSteps int     `json:"checkpoint_steps"`
	ResumeJob       string  `json:"resume_job,omitempty"`
}

func (a *App) train(w http.ResponseWriter, r *http.Request) {
	if !writeAccess(w, r) {
		return
	}
	in := TrainInput{Seed: 42, Steps: 20, LearningRate: .0002, Rank: 8, MaxLength: 256, CheckpointSteps: 10}
	if !decode(w, r, &in) {
		return
	}
	if in.Seed < 0 || in.Steps < 1 || in.Steps > 10000 || in.Rank < 1 || in.Rank > 64 || in.MaxLength < 16 || in.MaxLength > 1024 || in.LearningRate <= 0 || in.LearningRate > .01 || in.CheckpointSteps < 1 || in.CheckpointSteps > 10000 {
		fail(w, 422, "Invalid training parameters")
		return
	}
	model, dataset, ok := a.experimentInputs(w, r, in.Model, in.Dataset)
	if !ok {
		return
	}
	b, _ := json.Marshal(in)
	var p map[string]any
	json.Unmarshal(b, &p)
	p["model_config"] = model.Data
	p["dataset_version"] = dataset.Data["sha256"]
	p["records"] = dataset.Data["records"]
	if in.ResumeJob != "" {
		previous, e := a.store.Access(r.Context(), in.ResumeJob, "job", current(r), false)
		if e != nil {
			accessError(w, e)
			return
		}
		old, _ := previous.Data["payload"].(map[string]any)
		if str(previous.Data["operation"]) != "train" || str(old["model"]) != in.Model || str(old["dataset_version"]) != str(dataset.Data["sha256"]) {
			fail(w, 422, "Resume job must use the identical model and dataset version")
			return
		}
		for _, key := range []string{"seed", "rank", "max_length", "learning_rate"} {
			if fmt.Sprint(old[key]) != fmt.Sprint(p[key]) {
				fail(w, 422, "Resume parameters must match original job")
				return
			}
		}
	}
	a.submit(w, r, "train", p)
}
func (a *App) experimentInputs(w http.ResponseWriter, r *http.Request, modelID, datasetID string) (Record, Record, bool) {
	m, e := a.store.Access(r.Context(), modelID, "model", current(r), false)
	if e != nil {
		accessError(w, e)
		return m, Record{}, false
	}
	d, e := a.store.Access(r.Context(), datasetID, "dataset", current(r), false)
	if e != nil {
		accessError(w, e)
		return m, d, false
	}
	return m, d, true
}
func (a *App) submit(w http.ResponseWriter, r *http.Request, op string, p map[string]any) {
	row, e := a.enqueue(r.Context(), current(r), op, p)
	if e != nil {
		fail(w, 409, e.Error())
		return
	}
	respond(w, 202, publicRecord(row))
}
func (a *App) evaluate(w http.ResponseWriter, r *http.Request) {
	if !writeAccess(w, r) {
		return
	}
	var in struct {
		Model        string `json:"model"`
		Dataset      string `json:"dataset"`
		AdapterJob   string `json:"adapter_job"`
		Seed         int    `json:"seed"`
		MaxNewTokens int    `json:"max_new_tokens"`
	}
	in.Seed = 42
	in.MaxNewTokens = 64
	if !decode(w, r, &in) {
		return
	}
	if in.Seed < 0 || in.MaxNewTokens < 1 || in.MaxNewTokens > 512 {
		fail(w, 422, "Invalid evaluation parameters")
		return
	}
	m, d, ok := a.experimentInputs(w, r, in.Model, in.Dataset)
	if !ok {
		return
	}
	j, e := a.store.Access(r.Context(), in.AdapterJob, "job", current(r), false)
	if e != nil {
		accessError(w, e)
		return
	}
	p, _ := j.Data["payload"].(map[string]any)
	if j.Status != "succeeded" || str(j.Data["operation"]) != "train" || str(p["model"]) != in.Model {
		fail(w, 422, "Adapter must be a successful training job for this model")
		return
	}
	for _, rec := range array(d.Data["records"]) {
		obj, _ := rec.(map[string]any)
		if str(obj["prompt"]) == "" || str(obj["completion"]) == "" {
			fail(w, 422, "Evaluation requires prompt/completion records")
			return
		}
	}
	a.submit(w, r, "evaluate", map[string]any{"model": in.Model, "model_config": m.Data, "records": d.Data["records"], "dataset_version": d.Data["sha256"], "adapter_job": in.AdapterJob, "seed": in.Seed, "max_new_tokens": in.MaxNewTokens})
}
func (a *App) benchmark(w http.ResponseWriter, r *http.Request) {
	if !writeAccess(w, r) {
		return
	}
	var in struct {
		Size        int    `json:"size"`
		Dtype       string `json:"dtype"`
		Warmup      int    `json:"warmup"`
		Repetitions int    `json:"repetitions"`
		Seed        int    `json:"seed"`
	}
	in.Size = 1048576
	in.Dtype = "float32"
	in.Warmup = 25
	in.Repetitions = 100
	in.Seed = 42
	if !decode(w, r, &in) {
		return
	}
	if in.Size < 1024 || in.Size > 16777216 || (in.Dtype != "float16" && in.Dtype != "float32") || in.Warmup < 5 || in.Warmup > 1000 || in.Repetitions < 10 || in.Repetitions > 10000 || in.Seed < 0 {
		fail(w, 422, "Invalid benchmark configuration")
		return
	}
	b, _ := json.Marshal(in)
	var p map[string]any
	json.Unmarshal(b, &p)
	a.submit(w, r, "benchmark", p)
}
