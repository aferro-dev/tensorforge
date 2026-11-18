package platform

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
type ChatInput struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Stream      bool      `json:"stream"`
	MaxTokens   int       `json:"max_tokens"`
	Temperature float64   `json:"temperature"`
	Seed        *int      `json:"seed,omitempty"`
}

func (a *App) chat(w http.ResponseWriter, r *http.Request) {
	if !writeAccess(w, r) {
		return
	}
	in := ChatInput{MaxTokens: 256, Temperature: .7}
	if !decode(w, r, &in) {
		return
	}
	if e := validateChat(in); e != nil {
		fail(w, 422, e.Error())
		return
	}
	a.serveChat(w, r, in)
}
func validateChat(in ChatInput) error {
	if in.Model == "" || len(in.Messages) == 0 || len(in.Messages) > 100 || in.MaxTokens < 1 || in.MaxTokens > 2048 || in.Temperature < 0 || in.Temperature > 2 {
		return errors.New("Invalid model, messages or generation parameters")
	}
	total := 0
	for _, m := range in.Messages {
		if m.Role != "system" && m.Role != "user" && m.Role != "assistant" {
			return errors.New("Invalid message role")
		}
		total += len(m.Content)
	}
	if total > 64000 {
		return errors.New("Messages exceed 64000 bytes")
	}
	if in.Seed != nil && *in.Seed < 0 {
		return errors.New("Seed must be nonnegative")
	}
	return nil
}
func (a *App) acquire(ctx context.Context) (time.Duration, func(), error) {
	start := time.Now()
	if err := ctx.Err(); err != nil { return 0, nil, err }
	select {
	case a.waiting <- struct{}{}:
	default:
		return 0, nil, errors.New("Inference queue full")
	}
	releaseWait := func() { <-a.waiting }
	timer := time.NewTimer(a.cfg.QueueTimeout)
	defer timer.Stop()
	select {
	case a.slots <- struct{}{}:
		if err := ctx.Err(); err != nil { <-a.slots; releaseWait(); return 0, nil, err }
		return time.Since(start), func() { <-a.slots; releaseWait() }, nil
	case <-ctx.Done():
		releaseWait()
		return 0, nil, ctx.Err()
	case <-timer.C:
		releaseWait()
		return 0, nil, errors.New("Inference queue timeout")
	}
}
func (a *App) serveChat(w http.ResponseWriter, r *http.Request, in ChatInput) {
	model, e := a.store.Get(r.Context(), in.Model)
	if e != nil || model.Kind != "model" || model.Status != "running" {
		fail(w, 404, "Model instance is not running")
		return
	}
	start := time.Now()
	status := 200
	tokens := 0
	var ttft *float64
	var queue time.Duration
	defer func() {
		duration := time.Since(start)
		slog.Info("inference", "request_id", requestID(r), "workload_id", in.Model, "actor", current(r).ID, "status", status, "duration_ms", duration.Milliseconds(), "output_tokens", tokens)
		var throughput *float64
		if ttft != nil && tokens > 1 && float64(duration.Microseconds())/1000 > *ttft {
			v := float64(tokens-1) / ((float64(duration.Microseconds())/1000 - *ttft) / 1000)
			throughput = &v
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := a.store.SaveRequest(ctx, RequestMetric{ID:requestID(r),Actor:current(r).ID,Model:in.Model,Status:status,QueueMS:float64(queue.Microseconds())/1000,TTFTMS:ttft,DurationMS:float64(duration.Microseconds())/1000,OutputTokens:tokens,Throughput:throughput}); err != nil {
			slog.Error("persist request metrics", "request_id", requestID(r), "error", err)
		}
	}()
	queue, release, e := a.acquire(r.Context())
	if e != nil {
		status = 429
		w.Header().Set("Retry-After", "2")
		fail(w, 429, e.Error())
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(r.Context(), 180*time.Second)
	defer cancel()
	b, _ := json.Marshal(in)
	req, _ := http.NewRequestWithContext(ctx, "POST", a.cfg.AIURL+"/chat", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+a.cfg.ServiceKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", requestID(r))
	resp, e := a.client.Do(req)
	if e != nil {
		status = 502
		fail(w, 502, e.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		status = resp.StatusCode
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 65536))
		fail(w, status, string(body))
		return
	}
	if !in.Stream {
		var out map[string]any
		if e = json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&out); e != nil {
			status = 502
			fail(w, 502, "Malformed inference response")
			return
		}
		usage, _ := out["usage"].(map[string]any)
		tokens = int(number(usage["completion_tokens"]))
		respond(w, 200, out)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		status = 500
		fail(w, 500, "Streaming unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	completed := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			payload := strings.TrimPrefix(line, "data: ")
			if payload == "[DONE]" {
				completed = true
			} else {
				var frame map[string]any
				if json.Unmarshal([]byte(payload), &frame) == nil {
					usage, _ := frame["usage"].(map[string]any)
					if n := number(usage["completion_tokens"]); n > 0 {
						tokens = int(n)
					}
					for _, c := range array(frame["choices"]) {
						choice, _ := c.(map[string]any)
						delta, _ := choice["delta"].(map[string]any)
						if content := str(delta["content"]); content != "" && ttft == nil {
							v := float64(time.Since(start).Microseconds()) / 1000
							ttft = &v
						}
					}
				}
			}
		}
		if _, e = fmt.Fprintln(w, line); e != nil {
			status = 499
			return
		}
		flusher.Flush()
	}
	if r.Context().Err() != nil {
		status = 499
		return
	}
	if scanner.Err() != nil || !completed {
		status = 502
		fmt.Fprint(w, "event: error\ndata: {\"error\":{\"message\":\"Upstream stream interrupted\"}}\n\n")
		flusher.Flush()
	}
}
func (a *App) requests(w http.ResponseWriter, r *http.Request) {
	if !require(w, r, "administrator") {
		return
	}
	rows, e := a.store.Requests(r.Context())
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	respond(w, 200, rows)
}
func (a *App) metrics(w http.ResponseWriter, r *http.Request) {
	if !require(w, r, "administrator") {
		return
	}
	totals, err := a.store.RequestTotals(r.Context())
	if err != nil { fail(w, 503, "Metrics storage unavailable"); return }
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# TYPE tensorforge_requests_total counter\ntensorforge_requests_total %d\n# TYPE tensorforge_errors_total counter\ntensorforge_errors_total %d\ntensorforge_output_tokens_total %d\ntensorforge_queue_seconds_total %g\ntensorforge_request_seconds_total %g\ntensorforge_inference_active %d\ntensorforge_inference_waiting %d\n", totals.Total, totals.Failed, totals.Tokens, totals.QueueMS/1000, totals.DurationMS/1000, len(a.slots), max(0,len(a.waiting)-len(a.slots)))
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	gpu, e := a.ai(ctx, "/gpu", nil)
	if e == nil && gpu["available"] == true {
		fmt.Fprintf(w, "tensorforge_gpu_available 1\ntensorforge_gpu_used_bytes %g\ntensorforge_gpu_free_bytes %g\ntensorforge_gpu_utilization_percent %g\n", number(gpu["used_mb"])*1048576, number(gpu["free_mb"])*1048576, number(gpu["utilization_percent"]))
	} else {
		fmt.Fprintln(w, "tensorforge_gpu_available 0")
	}
}
