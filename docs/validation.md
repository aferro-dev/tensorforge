# Verifica a cura dell'operatore

Queste sono procedure da eseguire nel proprio ambiente container. Non sono state
eseguite sulla revisione finale e non richiedono installazioni di Go/Python
sull'host. Usare istanze dedicate ai test. La suite crea database MongoDB e namespace
Redis con nomi casuali, poi elimina solo il database e l'indice creati dal test.

## Test automatici

Nell'immagine Go con sorgenti e dipendenze disponibili:

```sh
go test -mod=mod -race -count=1 ./...
```

Fornire `TF_TEST_MONGODB_URI` e `TF_TEST_REDIS_URL`, quest'ultimo verso Redis con
Search. Senza entrambe le variabili, `TestIntegration` è esplicitamente saltato.
Il race detector richiede un compilatore C nell'immagine di test.

Copertura: RBAC, revoca chiavi, commit modello, enqueue e recovery, completamento
JSON e SSE con AI fixture, ACL MongoDB applicate ai filtri Redis KNN, citazioni, rifiuto/approvazione,
protezione CSRF, validazione dataset, SSR e sanitizzazione metadati condivisi.
Le fixture AI sono marcate come tali e non costituiscono prove GPU.

Nell'immagine Python con extra `test`, solo dipendenze base:

```sh
pytest -q -p no:cacheprovider
```

La suite usa un percorso `/tmp` e un segreto interno di test. Copre chunking,
provenienza ingestion, autenticazione interna, fallimento GPU senza risultati
fittizi, calcolo delle metriche, isolamento degli artefatti e path traversal.

## Smoke test manuale

1. Avviare il proprio stack e controllare `/healthz` e `/readyz` del gateway.
2. Verificare che la porta del servizio AI e del motore vLLM non siano pubblicate.
3. Fare login dal browser, creare developer e viewer. Controllare 401 senza key
   e 403 su registrazione modello con viewer. Revocare una key e riprovare il login.
4. Usare la dashboard in tutte le sezioni. Ripetere con JavaScript disabilitato:
   le pagine e i form SSR devono restare utilizzabili; il polling progress è HTMX.
5. Registrare un modello piccolo con SHA verificato. Scaricare e avviare.
   Se una dipendenza GPU manca, il job deve fallire con motivo e senza risultato.
6. Inviare chat JSON e SSE. Interrompere un client durante lo stream: controllare
   che il lavoro non continui indefinitamente e che la slot si liberi.
7. Inviare più richieste delle soglie concurrency+queue; verificare 429 e
   successivo recupero. Consultare `/api/requests` per queue_ms, TTFT e usage.
8. Caricare un documento UTF-8 e un PDF con testo. Verificare stato ready, numero
   chunk ed embedding revision. Un PDF solo immagine deve produrre errore chiaro.
9. Cercare da due utenti: il secondo non vede il documento finché l'owner non
   aggiunge il suo ID alle ACL. Revocare la condivisione e verificare la sparizione.
10. Richiedere una risposta grounded. Ogni indice deve riferirsi a un passaggio
    restituito. Valutare separatamente la correttezza delle affermazioni.
11. Creare un agente con write_note. Prima dell'approvazione nessuna nota deve
    esistere; dopo il rifiuto il piano resta ineseguito. Ripetere con approvazione
    e verificare log input/output e rifiuto della seconda approvazione.
12. Caricare un dataset errato e controllare i numeri di riga. Caricarne uno valido
    con coppie prompt/completion e conservare il suo hash.
13. Arrestare inferenza. Eseguire un training breve su una base che rientri in
    memoria. Controllare loss, allocated/peak memory, eventi e checkpoint.
14. Cancellare dopo un checkpoint completo. Riprendere con gli stessi parametri
    e un totale step maggiore; verificare rifiuto di dataset/modello incompatibili.
15. Eseguire la valutazione baseline/adapter con input e seed uguali; scaricare
    predictions.json e verificare le metriche aggregate senza presupporre miglioramenti.
16. Eseguire benchmark float16 e float32, anche con size non multiplo di 256.
    Controllare correctness, raw samples, warmup, versioni, source SHA e formula speedup.
17. Tentare training durante inferenza: deve fallire per lease GPU occupata.
    Tentare modello con stima eccessiva: deve fallire prima dell'avvio del motore.
18. Riavviare gateway durante un job: storia e artefatti restano; lo stato running
    diventa interrupted. I queued non scompaiono. Verificare riconciliazione del motore.
19. Condividere un report con viewer: il report è leggibile, il corpus sorgente
    non appare nel payload. Tentare download manifest.json o `../`: rifiutati.
20. Verificare scrape Prometheus autenticato, metriche GPU reali e correlazione
    dei request ID con workload/model ID nei log del gateway.
21. Verificare che chunk ed embedding siano presenti solo negli hash Redis e che
    `FT.INFO` descriva FLAT/FLOAT32/DIM 384/COSINE. Un Redis senza Search deve
    produrre un errore di avvio esplicito, senza fallback a scansioni Go.
22. Da un'istanza di test senza dati importanti, verificare **Rebuild search index**
    dopo perdita del contenuto Redis: viene pubblicata una nuova generazione,
    conservando documento e permessi MongoDB. Vecchie generazioni non entrano nel KNN.
23. Avviare due gateway sullo stesso database di test: il secondo non deve ottenere
    la lease. Verificare rinnovo, rilascio ordinato e scadenza dopo arresto anomalo.
24. Verificare che note, chiavi e approvazioni contengano audit outbox persistente
    fino alla copia in `audit`, e che il trasferimento ripetuto non duplichi eventi.

Conservare per ogni prova data, commit del repository, immagini e versioni usate,
modello/revisione, dataset/hash, seed, configurazione, log e risultati reali.
