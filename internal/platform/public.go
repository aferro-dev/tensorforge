package platform

// Source data stays in the durable job manifest but is omitted from API/SSR views.
// Sharing a report does not implicitly share its uploaded document or training corpus.
func publicRecord(row Record) Record {
 if row.Kind != "job" { return row }
 data := make(map[string]any, len(row.Data))
 for key, value := range row.Data { data[key] = value }
 if payload, ok := data["payload"].(map[string]any); ok {
  copy := make(map[string]any, len(payload))
  for key, value := range payload {
   if key != "records" && key != "content" { copy[key] = value }
  }
  data["payload"] = copy
 }
 if str(data["operation"]) == "ingest" {
  if result, ok := data["result"].(map[string]any); ok {
   copy := make(map[string]any, len(result))
   for key, value := range result { if key != "chunks" { copy[key] = value } }
   data["result"] = copy
  }
 }
 row.Data = data
 return row
}
