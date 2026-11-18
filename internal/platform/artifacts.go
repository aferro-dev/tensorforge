package platform

import (
 "io"
 "net/http"
 "net/url"
 "path"
 "strings"
 "time"
)

// Artifact access follows the job ACL; internal manifests are never exposed.
func (a *App) artifact(w http.ResponseWriter, r *http.Request) {
 row, err := a.store.Access(r.Context(), r.PathValue("id"), "job", current(r), false)
 if err != nil { accessError(w, err); return }
 name := r.PathValue("path")
 if name == "" || path.IsAbs(name) || path.Clean(name) != name || strings.HasPrefix(name, "../") || strings.Contains(name, "\\") || name == "manifest.json" {
  fail(w, 422, "Invalid artifact path"); return
 }
 ctx, cancel := timeout(r.Context(), 5*time.Minute); defer cancel()
 target := a.cfg.AIURL + "/artifacts/" + row.ID + "?path=" + url.QueryEscape(name)
 req, err := http.NewRequestWithContext(ctx, "GET", target, nil)
 if err != nil { fail(w, 500, "Cannot construct artifact request"); return }
 req.Header.Set("Authorization", "Bearer " + a.cfg.ServiceKey)
 response, err := a.client.Do(req)
 if err != nil { fail(w, 502, "AI artifact storage unavailable"); return }
 defer response.Body.Close()
 if response.StatusCode != 200 { fail(w, response.StatusCode, "Artifact unavailable"); return }
 w.Header().Set("Content-Type", "application/octet-stream")
 w.Header().Set("Content-Disposition", "attachment; filename="+url.PathEscape(path.Base(name)))
 if length := response.Header.Get("Content-Length"); length != "" { w.Header().Set("Content-Length", length) }
 io.Copy(w, response.Body)
}

func (a *App) jobACL(w http.ResponseWriter, r *http.Request) {
 if !writeAccess(w, r) { return }
 a.mu.Lock()
 defer a.mu.Unlock()
 row, err := a.store.Access(r.Context(), r.PathValue("id"), "job", current(r), true)
 if err != nil { accessError(w, err); return }
 var in struct { Readers []string `json:"readers"` }
 if !decode(w, r, &in) { return }
 if in.Readers == nil { in.Readers = []string{} }
 if len(in.Readers) > 100 { fail(w, 422, "Maximum 100 readers"); return }
 for _, id := range in.Readers {
  if id == "bootstrap-admin" { continue }
  if _, err := a.store.KeyByID(r.Context(), id); err != nil {
   fail(w, 422, "Unknown reader"); return
  }
 }
 // Update only the ACL, preserving simultaneous worker progress and terminal state.
 err = a.store.SetReaders(r.Context(), row.ID, in.Readers, current(r).ID, "job.acl")
 if err != nil { fail(w, 500, "Cannot save permissions"); return }
 respond(w, 200, map[string]any{"id":row.ID,"readers":in.Readers})
}
