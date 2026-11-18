# API del gateway Go

Il gateway usa JSON salvo gli upload multipart e lo streaming SSE. Tutte le API
richiedono `Authorization: Bearer <chiave>`; in alternativa il browser usa una
sessione HttpOnly con token CSRF per le mutazioni. `/healthz`, `/readyz`, `/login`
e gli asset statici sono pubblici. Gli errori JSON contengono
`{"error":{"message":"...","type":"...","code":422}}`; gli errori di validazione
dataset contengono inoltre un array `issues` con le righe errate.

Administrator gestisce tutte le risorse. Developer crea workload e gestisce le
proprie risorse. Viewer legge registro, GPU e risultati esplicitamente condivisi.
Le risorse private non autorizzate restituiscono 404. La revoca di una chiave
invalida anche le sessioni browser che la usano.

## Endpoint

| Metodo e percorso | Accesso / comportamento |
|---|---|
| `GET /api/me` | Identità e ruolo |
| `GET /api/keys` | Administrator; elenca ID, nomi, ruoli e revoche |
| `POST /api/keys` | Administrator; crea chiave, restituisce il segreto una volta |
| `DELETE /api/keys/{id}` | Administrator; revoca |
| `GET /api/models` | Registro condiviso fra utenti autenticati |
| `POST /api/models` | Administrator; registra modello riproducibile |
| `POST /api/models/{id}/download` | Administrator; accoda download dello snapshot |
| `POST /api/models/{id}/start` | Administrator; accoda avvio vLLM |
| `POST /api/models/{id}/stop` | Administrator; accoda arresto e rilascio GPU |
| `GET /api/gpu` | NVML; `available:false` in assenza di GPU |
| `GET /v1/models` | Lista OpenAI-compatible delle istanze avviate |
| `POST /v1/chat/completions` | Developer/administrator; completamento o SSE |
| `GET /api/jobs` | Job accessibili, massimo 500 record recenti prima del filtro ACL |
| `GET /api/jobs/{id}` | Stato, metadati, risultato e progress disponibile |
| `POST /api/jobs/{id}/cancel` | Owner/administrator; cancellazione queued/running |
| `PUT /api/jobs/{id}/acl` | Owner/administrator; condivisione report |
| `GET /api/jobs/{id}/artifacts/{path}` | Download con ACL del job; vietati manifest interni e traversal |
| `GET /api/documents` | Documenti visibili all'utente |
| `POST /api/documents` | Developer/administrator; multipart `file` |
| `POST /api/documents/{id}/reindex` | Owner/administrator; ricostruisce l'indice Redis dal sorgente conservato nel job |
| `PUT /api/documents/{id}/acl` | Owner/administrator; condivisione documenti |
| `POST /api/retrieval/search` | Developer/administrator; Redis KNN filtrato tramite ACL MongoDB |
| `POST /api/retrieval/answer` | Developer/administrator; risposta con citazioni |
| `GET /api/datasets` | Dataset privati accessibili |
| `POST /api/datasets` | Developer/administrator; multipart `file` JSONL |
| `POST /api/training` | Accoda training LoRA |
| `POST /api/evaluations` | Accoda confronto baseline/adapter |
| `POST /api/benchmarks` | Accoda benchmark reale PyTorch/Triton |
| `GET /api/agents` | Configurazioni accessibili |
| `POST /api/agents` | Crea agente con strumenti approvati |
| `POST /api/agents/{id}/runs` | Esegue o sospende un piano esplicito |
| `GET /api/runs` | Run, input/output degli strumenti e stato approvazione |
| `POST /api/runs/{id}/approval` | Administrator; approva o rifiuta una volta |
| `POST /mcp` | JSON-RPC initialize, tools/list, tools/call |
| `GET /api/audit` | Administrator; ultimi 500 eventi |
| `GET /api/requests` | Administrator; ultime 500 misurazioni richiesta |
| `GET /metrics` | Administrator; formato Prometheus |

Le operazioni accodate restituiscono 202 con un record `kind:"job"`. Le chiamate
ripetute di enqueue creano nuovi job: non è previsto un header di idempotenza.
Gli stati terminali sono `succeeded`, `failed`, `cancelled`, `interrupted`.
Un errore hardware non produce risultati simulati.

## Payload

Creazione chiave:

```json
{"name":"Giulia","role":"developer"}
```

Registrazione modello; sostituire la revisione con un commit realmente verificato:

```json
{
  "model_id":"organization/model",
  "revision":"<SHA Hugging Face di 40 caratteri>",
  "license":"apache-2.0",
  "configuration":{},
  "estimated_vram_mb":4000,
  "max_model_len":2048,
  "dtype":"half"
}
```

`quantization` opzionale: `awq`, `gptq`, `bitsandbytes`. La compatibilità concreta
è verificata da vLLM durante l'avvio. Il download deve precedere l'avvio; modelli
non presenti nella cache locale falliscono esplicitamente.

Completamento chat:

```json
{
  "model":"<ID del registro>",
  "messages":[{"role":"user","content":"Spiega la vector addition"}],
  "max_tokens":128,
  "temperature":0.7,
  "stream":true,
  "seed":42
}
```

Sono supportati messaggi testuali system/user/assistant. Non sono supportati
multimodalità, function calling nel completamento chat, logprobs o ogni parametro
opzionale dell'intera API OpenAI. Le risposte non streaming sono inoltrate da
vLLM; gli stream inoltrano frame OpenAI e terminatore `data: [DONE]`.
Una chiusura del client cancella il contesto Go e chiude lo stream verso il motore.

Ricerca e risposta grounded:

```json
{"query":"Qual è la procedura di riavvio?","top_k":4}
```

```json
{"query":"Qual è la procedura di riavvio?","top_k":4,"model":"<ID>","max_tokens":256}
```

Ogni passaggio riporta ID documento/chunk, nome, posizione e score. Le citazioni
in risposta includono `citation`, l'indice numerico usato nel testo. Le referenze
vengono validate; non viene promessa la verifica semantica automatica.

Condivisione, per documenti o job:

```json
{"readers":["<ID chiave utente viewer>"]}
```

Un array vuoto revoca tutte le condivisioni. La condivisione di job nasconde i
record sorgente del dataset e il corpo del documento dalla rappresentazione del
job; condivide i risultati e gli artefatti, che possono contenere predizioni e
informazioni sensibili. La lettura del dataset sorgente resta indipendente.

Dataset JSONL valido per training e valutazione:

```jsonl
{"prompt":"Quanto fa 2+2?","completion":"4"}
{"prompt":"Capitale d'Italia?","completion":"Roma"}
```

Il formato `{"text":"..."}` è valido per training, non per valutazione.

Training:

```json
{"model":"<ID>","dataset":"<ID>","seed":42,"steps":20,"learning_rate":0.0002,"rank":8,"max_length":256,"checkpoint_steps":10}
```

Aggiungere `resume_job` per riprendere l'ultimo checkpoint completo compatibile.
Il modello deve essere già scaricato. La GPU deve essere libera da inferenza e
altri processi. Ogni checkpoint contiene adapter, optimizer, scaler, RNG e metadati.

Valutazione:

```json
{"model":"<ID>","dataset":"<ID>","adapter_job":"<training completato>","seed":42,"max_new_tokens":64}
```

Benchmark:

```json
{"size":1048576,"dtype":"float32","warmup":25,"repetitions":100,"seed":42}
```

Il risultato riporta latenze grezze in millisecondi, mediane e speedup
`median(pytorch) / median(triton)`. Non è un benchmark dell'intera inferenza LLM.

Agente e piano:

```json
{"name":"Research assistant","tools":["calculator","search","write_note"]}
```

```json
{"calls":[{"tool":"calculator","arguments":{"expression":"(2.4-1.8)/2.4*100"}},{"tool":"write_note","arguments":{"title":"Calculation","content":"Manual arithmetic example; not a measured benchmark."}}]}
```

Qualsiasi piano contenente scritture resta `awaiting_approval` per default.
L'amministratore invia `{"approve":true}` oppure `{"approve":false}`. Dopo
risoluzione, una seconda approvazione restituisce 409. Non sono eseguiti comandi
shell, eval, codice Python arbitrario o richieste a URL scelti dall'agente.

MCP tools-only:

```json
{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}
```

```json
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"calculator","arguments":{"expression":"2+2"}}}
```

`write_note` via MCP viene sempre rifiutato e rimanda al flusso agenti con
approvazione. Il trasporto non implementa sessioni MCP/SSE o discovery remota.
