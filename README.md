# TensorForge

Piattaforma self-hosted per gestire modelli open-weight, inferenza, knowledge base,
agenti controllati, esperimenti LoRA e benchmark PyTorch/Triton su una singola GPU.

**Go gestisce web, utenti e piattaforma. Python gestisce esclusivamente AI/LLM/GPU.**
Tutte le pagine sono renderizzate da Go con `html/template`; HTMX aggiorna navigazione,
form e avanzamento dei job. Non è presente una SPA o un frontend Python.

## Struttura

| Percorso | Responsabilità |
|---|---|
| `cmd/server` | Avvio e arresto del gateway Go |
| `internal/platform` | API, autenticazione, RBAC, MongoDB, code, audit, Redis RAG e runtime agenti |
| `web/templates`, `web/static` | SSR, form HTMX, CSS |
| `tensorforge/ai.py` | Servizio AI interno, processi vLLM, embedding, parsing, gestione GPU |
| `tensorforge/experiments.py` | Worker isolato per LoRA, valutazioni e kernel Triton |
| `tensorforge/download.py` | Download cancellabile di snapshot Hugging Face |
| `tests`, `internal/platform/platform_test.go` | Test Python e Go, incluse integrazioni MongoDB/Redis Search |
| `ops` | Configurazioni Prometheus e dashboard Grafana |
| `docs` | Contratti, copertura del PRD e procedure di verifica |

Il gateway conserva utenti, modelli, job, dataset, permessi e audit in MongoDB.
Redis conserva chunk ed embedding e li ricerca tramite un indice vettoriale FLAT
FLOAT32, 384 dimensioni, distanza COSINE. Il servizio AI usa una chiave
interna distinta da quelle degli utenti. Il processo vLLM ascolta solo su loopback
all'interno del servizio AI. Il browser comunica esclusivamente con Go.

## Stato della consegna

Il repository contiene l'implementazione dei flussi M1–M5. **La Definition of Done
che richiede esecuzioni reali GPU non è dichiarata raggiunta.** Non sono inclusi
risultati di inferenza, training o benchmark inventati. Le latenze 2,4 ms e 1,8 ms
citate nel PRD non sono state inserite come misurazioni della piattaforma.

Su richiesta dell'utente, la revisione finale viene consegnata come solo codice:
nessun ulteriore avvio di container, build, test o servizio. Sei test Python su una
versione precedente sono stati eseguiti; questo non certifica i sorgenti finali.
La suite Go/integrazione e le modifiche successive restano da eseguire.

La preparazione e l'esecuzione delle immagini sono lasciate all'operatore: non installare le
dipendenze dei progetti direttamente sull'host.

## Contratto di avvio

Per usare Air nel proprio ambiente di sviluppo, risolvere i nuovi driver e poi
avviare il gateway dalla root del progetto:

```sh
go mod tidy
go tool air -c .air.toml
```

Questi comandi sono lasciati all'operatore. `.air.toml` carica il `.env`; MongoDB
e Redis con Search devono essere già raggiungibili agli URL configurati.

Predisporre due immagini distinte, usando i manifest `go.mod`/`go.sum` e
`pyproject.toml`. Eseguire i seguenti comandi **all'interno delle rispettive immagini**:

- Go: `go run ./cmd/assets` per includere la dipendenza HTMX 2.0.4, poi
  `go build -mod=mod -o /app/server ./cmd/server`; includere `web/` nell'immagine. Il download
  degli asset avviene nella build scelta dall'operatore, mai all'avvio del server.
  `cmd/assets` supporta `-sha256` per verificare un digest indipendente e registra
  l'integrità dell'asset. Il browser non dipende da CDN a runtime.
- Python: installare il progetto e avviare
  `uvicorn tensorforge.ai:app --host 0.0.0.0 --port 8101 --workers 1`.
- Il pacchetto Python base gestisce protocollo, download e parsing. L'extra
  `retrieval` aggiunge Sentence Transformers; l'extra `gpu` aggiunge PyTorch,
  PEFT e Triton. Per inferenza installare anche `vllm==0.8.5` nell'immagine GPU.
- Il gateway deve essere una sola istanza; una lease MongoDB rinnovata impedisce
  due scheduler contemporanei. La perdita della lease interrompe i workload e
  rende il gateway non pronto. Dopo un arresto anomalo la lease scade entro 90
  secondi. Anche il servizio AI usa un solo processo ASGI.

Servono MongoDB e Redis con Redis Search e supporto agli indici vettoriali
(`FT.CREATE`/`FT.SEARCH`), per esempio Redis con Search incluso o Redis Stack.
Redis senza tali comandi viene rifiutato chiaramente all'avvio. MongoDB standalone
è sufficiente: le mutazioni sensibili usano aggiornamenti atomici e audit outbox,
senza richiedere transazioni multi-documento o un replica set. Abilitare persistenza
e backup per entrambi: Redis contiene dati RAG, non è una cache eliminabile.
Snapshot, checkpoint, eventi e report sono nel volume AI privato `/data`.
Il servizio AI non deve essere pubblicato all'esterno.

| Variabile | Servizio | Descrizione |
|---|---|---|
| `MONGODB_URI` | Go | Default `mongodb://localhost:27017` |
| `TF_MONGO_DATABASE` | Go | Default `tensorforge`, database gestionale dedicato |
| `REDIS_URL` | Go | Default `redis://localhost:6379/0`, Redis con Search |
| `TF_REDIS_NAMESPACE` | Go | Default `tensorforge`, prefisso chiavi e indice vettoriale |
| `TF_ADMIN_KEY` | Go | Chiave bootstrap amministratore, almeno 32 caratteri casuali |
| `TF_SERVICE_KEY` | Entrambi | Segreto interno distinto, almeno 32 caratteri |
| `TF_AI_URL` | Go | Default `http://ai:8101` |
| `TF_ADDR` | Go | Default `:8000` |
| `TF_WEB_DIR` | Go | Default `web`, directory template e asset |
| `TF_CONCURRENCY` | Go | Default 1, intervallo 1–16 |
| `TF_QUEUE_SIZE` | Go | Default 8, massimo 100 richieste in attesa |
| `TF_WRITE_APPROVAL` | Go | Default `true`; approvazione amministratore per note |
| `TF_SECURE_COOKIES` | Go | `true` quando esposto tramite HTTPS |
| `TF_DATA_DIR` | Python | Default `/data`, volume scrivibile |
| `HF_HOME` | Python | Impostare `/data/huggingface` per cache persistente |
| `TF_EMBEDDING_MODEL` | Python | Default `sentence-transformers/all-MiniLM-L6-v2` |
| `TF_EMBEDDING_REVISION` | Python | SHA immutabile di 40 caratteri, obbligatorio per embedding |
| `TF_VLLM_START_TIMEOUT` | Python | Timeout avvio motore, default 300 secondi |

Usare segreti casuali distinti, senza committare `.env`. Go crea indici MongoDB e
l'indice Redis idempotentemente; gli utenti dei servizi devono poterli gestire.
Gli URL localhost nel `.env` sono adatti a un gateway raggiunto tramite Air nel
proprio ambiente di sviluppo; nelle reti container usare i nomi dei rispettivi
servizi. I driver MongoDB/Redis vengono risolti dal proprio ambiente di build:
nessuna dipendenza viene installata automaticamente sull'host dall'applicazione.

La GPU deve già essere accessibile dal runtime container. La piattaforma non
installa o modifica driver, toolkit NVIDIA o configurazione dell'host.

## Uso dalla dashboard

Aprire `/login` sul gateway e inserire la chiave bootstrap. In **Users** creare
chiavi individuali administrator, developer o viewer; il segreto è mostrato solo
alla creazione. Le chiavi possono essere revocate. Le sessioni browser usano
cookie HttpOnly/SameSite e token CSRF; tutte le mutazioni applicano RBAC.

1. **Models:** registrare repository, SHA del commit, licenza, configurazione e
   stima VRAM. Eseguire Download, poi Start. Gli errori sono conservati nei job.
2. **Inference:** inviare una richiesta al modello avviato usando l'ID del
   registro. L'API `/v1/chat/completions` offre anche streaming SSE e cancellazione
   alla disconnessione. `/v1/models` elenca le istanze attive.
3. **Documents:** caricare `.txt`, `.md` o PDF testuali, massimo 10 MiB.
   L'ingestion genera chunk sovrapposti ed embedding CPU normalizzati a 384
   dimensioni. Chunk ed embedding sono pubblicati in Redis; documenti, stato e
   permessi rimangono in MongoDB. Documenti e dataset sono privati per default.
   **Rebuild search index** ricostruisce i vettori dal sorgente privato conservato
   nel job, anche dopo la perdita dei dati Redis o un cambio di revisione embedding.
4. **Retrieval:** cercare passaggi o generare risposte citate. MongoDB seleziona
   le generazioni autorizzate; il filtro TAG Redis limita il KNN prima del limite
   dei risultati. Il gateway ricontrolla le ACL prima della costruzione del contesto.
   La risposta viene trattenuta se il modello omette citazioni o usa indici sconosciuti.
5. **Agents:** scegliere fra calculator, search e write_note. Fornire un piano
   esplicito di massimo otto chiamate. Le scritture attendono un amministratore
   in **Runs**; il rifiuto non esegue nessuna azione del piano.
6. **Datasets:** caricare JSONL con `text` oppure `prompt`/`completion`.
   La validazione segnala le righe errate e registra SHA-256 del contenuto.
7. **Training:** arrestare l'inferenza, poi avviare LoRA con modello e dataset.
   Configurazione, seed, loss, memoria, checkpoint e metadati sono conservati.
   Il resume richiede modello, dataset e parametri compatibili; `steps` indica
   il numero totale di step desiderato, non gli step aggiuntivi.
8. **Evaluations:** scegliere un adapter completato e un dataset con coppie
   prompt/completion. Baseline e adapter ricevono gli stessi input e seed,
   con generazione greedy; sono registrati exact match, token F1 e predizioni.
9. **Benchmarks:** misurare vector addition PyTorch/Triton. I report contengono
   campioni CUDA-event, warmup, dtype, shape, correttezza, versioni e hash del
   sorgente. Lo speedup è calcolato dalle mediane misurate.
10. **Jobs:** ispezionare stato, cancellare lavori e condividere report con ID
    utente, compresi viewer. Le ACL di un job non condividono implicitamente il
    dataset sorgente. Gli artefatti sono disponibili dalle API autenticate.

La documentazione API è in [docs/api.md](docs/api.md). Le verifiche riproducibili
sono in [docs/validation.md](docs/validation.md) e la copertura delle user story
in [docs/requirements.md](docs/requirements.md).

## Limiti operativi espliciti

- La memoria di una RTX 3060 Ti non consente qualsiasi modello. Le stime VRAM
  sono dichiarate dall'amministratore: non costituiscono una garanzia. Admission
  check, riserva di 768 MiB e limiti dell'allocator riducono il rischio; gli OOM
  e le architetture non supportate diventano job falliti con motivazione.
- Inferenza, training, valutazione e benchmark possiedono una lease GPU esclusiva.
  Gli embedding operano su CPU. La coda dei job è limitata a 64 lavori pendenti.
- Il runner LoRA usa batch 1, fp16, gradient checkpointing e adapter all-linear.
  Training/evaluation di basi quantizzate sono rifiutati chiaramente: QLoRA non
  è implementato. L'inferenza può usare le quantizzazioni supportate da vLLM.
- Gli agenti eseguono piani espliciti; non è implementato un planner LLM autonomo.
  Il runtime è Go, in accordo con il vincolo richiesto, e non dipende da LangGraph.
  `/mcp` offre il sottoinsieme JSON-RPC tools; non è un client per server MCP arbitrari.
- Sono supportati PDF con testo; OCR, HTML, Office e embedding di immagini non
  sono implementati. L'encoding degli upload testuali è UTF-8.
- Validare gli indici delle citazioni non dimostra la correttezza semantica delle
  affermazioni. Questo aspetto richiede valutazioni documentali separate.
- TTFT e throughput di generazione sono disponibili per lo streaming con usage;
  per richieste non streaming TTFT rimane nullo. Non viene stimato artificialmente.
- NVML riporta memoria usata dell'intero dispositivo. Le allocazioni del processo
  PyTorch sono registrate separatamente negli eventi dei worker.
- Prometheus espone metriche aggregate; i log e le righe request_metrics correlano
  request ID e modello. Non è incluso un backend OTLP/Jaeger.
- I job in esecuzione al riavvio sono marcati interrupted, non ripresi alla cieca.
  I job queued restano nella coda. Le approvazioni in attesa restano pendenti.
- Non sono implementati SSO, multi-node, model sharding, object storage remoto,
  retention automatica e sandbox per plugin arbitrari. Gli strumenti Go approvati
  non dispongono di shell né esecuzione di codice o comandi esterni.
- MongoDB limita la dimensione dei documenti: l'applicazione rifiuta record oltre
  15 MiB con un errore esplicito. I vettori non sono duplicati nei job MongoDB.
- L'indice Redis FLAT esegue KNN esatto. Le generazioni autorizzate sono suddivise
  in gruppi di 128 documenti e i risultati migliori vengono uniti. Non è previsto
  un fallback a scansioni vettoriali Go o a database alternativi.

## Riferimenti tecnici

Implementazioni basate sulle interfacce documentate di
[vLLM OpenAI server](https://docs.vllm.ai/en/v0.8.5/serving/openai_compatible_server.html),
[PEFT LoRA](https://huggingface.co/docs/peft/en/package_reference/lora) e
[MongoDB Go Driver](https://www.mongodb.com/docs/drivers/go/current/get-started/) e
[Redis Vector Search](https://redis.io/docs/latest/develop/ai/search-and-query/vectors/).
