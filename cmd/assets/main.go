// assets vendors the pinned HTMX source during an operator-controlled image build.
// It is never invoked by the gateway and does not install anything on the host.
package main

import (
 "crypto/sha256"
 "encoding/hex"
 "encoding/json"
 "flag"
 "fmt"
 "io"
 "net/http"
 "os"
 "path/filepath"
 "time"
)

func main() {
 destination := flag.String("out", "web/static", "asset output directory")
 expected := flag.String("sha256", "", "optional independently verified source digest")
 flag.Parse()
 if err := vendor(*destination, *expected); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}

func vendor(destination, expected string) error {
 const source = "https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js"
 client := &http.Client{Timeout:30*time.Second}
 response, err := client.Get(source)
 if err != nil { return err }
 defer response.Body.Close()
 if response.StatusCode != http.StatusOK { return fmt.Errorf("asset download: %s", response.Status) }
 data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
 if err != nil { return err }
 if len(data) < 10000 || len(data) >= 1<<20 { return fmt.Errorf("unexpected HTMX source size") }
 digest := sha256.Sum256(data)
 actual := hex.EncodeToString(digest[:])
 if expected != "" && actual != expected { return fmt.Errorf("HTMX digest mismatch: got %s", actual) }
 if err := os.MkdirAll(destination,0755); err != nil { return err }
 manifestPath := filepath.Join(destination,"htmx.integrity.json")
 if previous, err := os.ReadFile(manifestPath); err == nil {
  var manifest map[string]string
  if json.Unmarshal(previous,&manifest) != nil || manifest["sha256"] != actual {
   return fmt.Errorf("download differs from existing pinned asset manifest")
  }
 }
 target := filepath.Join(destination,"htmx.min.js")
 if err := os.WriteFile(target+".tmp",data,0644); err != nil { return err }
 if err := os.Rename(target+".tmp",target); err != nil { return err }
 manifest, _ := json.MarshalIndent(map[string]string{"version":"2.0.4","source":source,"sha256":actual},"","  ")
 if err := os.WriteFile(manifestPath,manifest,0644); err != nil { return err }
 fmt.Printf("Vendored HTMX 2.0.4, sha256=%s\n",actual)
 return nil
}
