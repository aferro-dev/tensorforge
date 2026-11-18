# Copertura PRD 1.0

Questa tabella descrive implementazioni e prove previste, non certifica esecuzioni
GPU. La revisione finale non è stata compilata o testata, come richiesto.

| Storia | Implementazione | Verifica prevista / limite |
|---|---|---|
| US-01 | Registro Go, commit SHA obbligatorio, licenza, configurazione, job download | Integrazione API; download reale da verificare |
| US-02 | Job avvio/arresto vLLM, lease esclusiva NVML, startup timeout, log | GPU manuale; controllare VRAM dopo Stop |
| US-03 | NVML device utilization/free/used, workload ID; eventi allocator PyTorch | Nessun numero simulato; NVML used non equivale a torch allocated |
| US-04 | `/v1/chat/completions`, `/v1/models`, Bearer/RBAC, errori strutturati | Integrazione Go con AI fixture; smoke test vLLM necessario |
| US-05 | SSE, usage finale, propagazione cancellazione e chiusura upstream | Test fixture stream; disconnessione reale da verificare |
| US-06 | Concurrency 1 default, coda limitata, timeout; metriche persistite | Test admission; TTFT solo stream |
| US-07 | Upload testo/Markdown/PDF, chunking, embedding CPU, indice vettoriale Redis | Unit test chunking/ingest; Sentence Transformers reale da verificare |
| US-08 | Passaggi citati, prompt grounded, indici citazioni validati | Fixture grounded; valutazione semantica manuale separata |
| US-09 | ACL MongoDB → filtro TAG Redis prima del KNN/LIMIT, ricontrollo dopo ricerca | Integrazione MongoDB/Redis con documenti identici di utenti diversi |
| US-10 | Tre tool Go approvati, schema/argomenti limitati, piano max 8 step, log | Test calculator/validation; planner autonomo escluso |
| US-11 | Stato awaiting_approval, approvazione admin una sola volta, rifiuto senza esecuzione | Integrazione run/rejection/duplicate approval |
| US-12 | JSONL validato, hash dataset, configurazione job, seed, revisione modello | Test righe invalide; nessun dataset implicito condiviso |
| US-13 | Eventi loss/runtime/memoria, checkpoint adapter/optimizer/RNG, resume | Training GPU manuale; checkpoint parziali ignorati |
| US-14 | Baseline/adapter stessi input e seed, greedy, exact match/token F1, predizioni | Unit test score; esperimento reale necessario |
| US-15 | Kernel vector-add Triton, baseline torch.add con output preallocato | GPU manuale; confronto tolleranze/tail masking |
| US-16 | Raw samples CUDA events, versioni, device, shape, seed, hash sorgente, median speedup | Ispezione report; nessuna performance precompilata |
| US-17 | SSR health, Prometheus, dashboard Grafana, request ID/workload nei log | Scrape da verificare; nessun exporter OTLP |
| US-18 | Stato MongoDB, Redis persistente per RAG, volume artefatti, recovery running→interrupted | Test recovery; riavvio servizi manuale |

## Scelte rispetto all'architettura iniziale

- FastAPI è confinato al servizio AI interno; il gateway e tutta la parte gestionale
  sono Go, secondo la richiesta successiva dell'utente.
- SSR `html/template` e HTMX sostituiscono qualsiasi SPA.
- Il runtime agenti è Go con piani espliciti, strumenti senza shell e audit;
  LangGraph non è incluso. MCP è un endpoint tools JSON-RPC limitato.
- MongoDB sostituisce la persistenza relazionale. Redis Search conserva e ricerca
  i vettori; non è usato come cache del gestionale. Le integrazioni usano entrambi
  i servizi reali. Le fixture AI restano nei test.
- Il filesystem persistente soddisfa lo storage locale; object storage remoto
  non è incluso.

## Definition of Done rimanente

Eseguire la suite finale, servire almeno un modello reale, completare retrieval
con embedding reali, approvare/eseguire un agente, addestrare/riprendere un adapter,
valutarlo e generare un report Triton sulla GPU target. I limiti documentati
(QLoRA, planner autonomo, protocolli MCP avanzati e OTLP) richiedono sviluppo
aggiuntivo se diventano criteri obbligatori.
