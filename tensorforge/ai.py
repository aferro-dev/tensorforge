"""Internal AI/GPU service. User authorization, jobs and business state live in Go."""
import asyncio
import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from .gpu import GPU


class Operation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(pattern=r"^[a-f0-9]{24}$")
    operation: str
    payload: dict


class EmbedInput(BaseModel):
    texts: list[str] = Field(min_length=1, max_length=64)


def chunk_text(text, size=1400, overlap=200):
    text = text.strip()
    if not text:
        raise ValueError("Document has no extractable text (scanned PDFs require OCR)")
    if len(text) > 2_000_000:
        raise ValueError("Extracted document exceeds 2 million characters")
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + size, len(text))
        if end < len(text):
            boundary = text.rfind(" ", start + size // 2, end)
            if boundary > start:
                end = boundary
        chunks.append(text[start:end])
        if end == len(text):
            break
        start = end - overlap
    if len(chunks) > 2000:
        raise ValueError("Document exceeds 2000 chunks")
    return chunks


class AIState:
    def __init__(self):
        self.data = Path(os.getenv("TF_DATA_DIR", "/data"))
        self.data.mkdir(parents=True, exist_ok=True)
        self.gpu = GPU()
        self.operations = asyncio.Lock()
        self.embed_lock = asyncio.Lock()
        self.embedder = None
        self.embedding_id = None
        self.process = None
        self.model = None
        self.log = None
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(180, connect=10))
        self.children = set()

    def metrics(self):
        if self.process and self.process.poll() is not None:
            self.gpu.release(self.model)
            self.process = None
            self.model = None
        return self.gpu.metrics()

    async def stop(self):
        if self.process:
            await terminate(self.process)
            self.process = None
        if self.model:
            self.gpu.release(self.model)
        self.model = None
        if self.log:
            self.log.close()
            self.log = None

    async def embeddings(self, texts):
        if any(not isinstance(t, str) or len(t) > 16000 for t in texts):
            raise ValueError("Embedding inputs must be strings of at most 16000 characters")
        async with self.embed_lock:
            if self.embedder is None:
                revision = os.getenv("TF_EMBEDDING_REVISION", "")
                if not re.fullmatch(r"[a-fA-F0-9]{40}", revision):
                    raise ValueError("TF_EMBEDDING_REVISION must pin a 40-character Hugging Face commit")
                model = os.getenv("TF_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
                def load():
                    from sentence_transformers import SentenceTransformer
                    return SentenceTransformer(model, revision=revision, device="cpu", trust_remote_code=False)
                self.embedder = await asyncio.to_thread(load)
                self.embedding_id = f"{model}@{revision}"
                if self.embedder.get_sentence_embedding_dimension() != 384:
                    self.embedder = None
                    raise ValueError("Redis vector index requires a 384-dimensional embedding model")
            vectors = await asyncio.to_thread(
                self.embedder.encode, texts, normalize_embeddings=True, show_progress_bar=False,
                batch_size=16,
            )
            return {"vectors": vectors.tolist(), "embedding_id": self.embedding_id}


async def terminate(process):
    if process.poll() is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        for _ in range(50):
            if process.poll() is not None:
                break
            await asyncio.sleep(.1)
        if process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        await asyncio.to_thread(process.wait)


def safe_dir(root, job_id):
    if not re.fullmatch(r"[a-f0-9]{24}", job_id):
        raise ValueError("Invalid artifact ID")
    directory = root / "jobs" / job_id
    directory.mkdir(parents=True, exist_ok=True)
    return directory


async def watch_disconnect(request, task):
    while not task.done():
        if await request.is_disconnected():
            task.cancel()
            return
        await asyncio.sleep(.2)


def create_app(state=None):
    state = state or AIState()
    service_key = os.getenv("TF_SERVICE_KEY", "")
    if len(service_key) < 32:
        raise RuntimeError("TF_SERVICE_KEY must contain at least 32 characters")

    @asynccontextmanager
    async def lifespan(app):
        yield
        for child in list(state.children):
            await terminate(child)
        await state.stop()
        await state.http.aclose()

    app = FastAPI(title="TensorForge internal AI", lifespan=lifespan, docs_url=None, redoc_url=None)
    app.state.ai = state

    async def authorized(request: Request):
        value = request.headers.get("authorization", "").removeprefix("Bearer ")
        if not secrets.compare_digest(value, service_key):
            raise HTTPException(401, "Invalid internal service key")

    @app.get("/health")
    async def health():
        return {"status": "ready"}

    @app.get("/gpu", dependencies=[Depends(authorized)])
    async def gpu():
        return await asyncio.to_thread(state.metrics)

    @app.post("/reset", dependencies=[Depends(authorized)])
    async def reset():
        if state.operations.locked():
            raise HTTPException(409, "An AI operation is still running")
        await state.stop()
        return {"status": "stopped"}

    @app.post("/embed", dependencies=[Depends(authorized)])
    async def embed(body: EmbedInput):
        try:
            return await state.embeddings(body.texts)
        except Exception as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.get("/progress/{job_id}", dependencies=[Depends(authorized)])
    async def progress(job_id: str):
        try:
            directory = safe_dir(state.data, job_id)
            events = directory / "events.jsonl"
            lines = events.read_text().splitlines()[-200:] if events.exists() else []
            return {"events": [json.loads(line) for line in lines],
                    "artifacts": sorted(str(p.relative_to(directory)) for p in directory.rglob("*")
                                        if p.is_file() and p.name != "manifest.json")[:500]}
        except (ValueError, json.JSONDecodeError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.get("/artifacts/{job_id}", dependencies=[Depends(authorized)])
    async def artifact(job_id: str, path: str):
        try:
            directory = safe_dir(state.data, job_id).resolve()
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        target = (directory / path).resolve()
        if (not target.is_relative_to(directory) or not target.is_file()
                or target.name == "manifest.json" or target.is_symlink()):
            raise HTTPException(404, "Artifact not found")
        return FileResponse(target, filename=target.name, media_type="application/octet-stream")

    @app.post("/operations", dependencies=[Depends(authorized)])
    async def operations(body: Operation, request: Request):
        if state.operations.locked():
            raise HTTPException(409, "Another AI operation is active")
        async with state.operations:
            task = asyncio.create_task(run_operation(state, body))
            watcher = asyncio.create_task(watch_disconnect(request, task))
            try:
                return await task
            except asyncio.CancelledError as exc:
                raise HTTPException(499, "AI operation cancelled") from exc
            except Exception as exc:
                raise HTTPException(422, str(exc)) from exc
            finally:
                watcher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watcher

    @app.post("/chat", dependencies=[Depends(authorized)])
    async def chat(request: Request):
        body = await request.json()
        if not state.process or state.process.poll() is not None or body.get("model") != state.model:
            raise HTTPException(409, "Model is not running in AI service; restart the instance")
        if body.get("stream"):
            body["stream_options"] = {"include_usage": True}
        upstream = state.http.build_request("POST", "http://127.0.0.1:8100/v1/chat/completions", json=body)
        try:
            response = await state.http.send(upstream, stream=True)
        except httpx.HTTPError as exc:
            raise HTTPException(502, str(exc)) from exc
        if response.status_code >= 400:
            error = (await response.aread()).decode(errors="replace")
            await response.aclose()
            raise HTTPException(response.status_code, error)
        async def stream():
            try:
                async for chunk in response.aiter_bytes():
                    if await request.is_disconnected():
                        break
                    yield chunk
            finally:
                # Closing the upstream socket causes vLLM to abort the request on disconnect.
                await response.aclose()
        return StreamingResponse(stream(), media_type="text/event-stream" if body.get("stream") else "application/json")

    return app


async def run_operation(state, body):
    p = body.payload
    op = body.operation
    directory = safe_dir(state.data, body.id)
    started = time.monotonic()
    if op == "model_stop":
        if state.model and state.model != p["model"]:
            raise ValueError("A different model owns the GPU; cannot stop it with this ID")
        await state.stop()
        return {"stopped": True}
    if op == "model_download":
        config = p["configuration"]
        # A subprocess allows cancellation to stop downloads, unlike an abandoned thread.
        process = None
        try:
            with (directory / "download.log").open("wb") as log:
                process = subprocess.Popen(
                    [sys.executable, "-m", "tensorforge.download", config["model_id"],
                     config["revision"], str(directory / "download.json")],
                    stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                )
                state.children.add(process)
                while process.poll() is None:
                    await asyncio.sleep(.25)
                if process.returncode != 0:
                    raise RuntimeError((directory / "download.log").read_text(errors="replace")[-4000:])
            return json.loads((directory / "download.json").read_text())
        finally:
            if process:
                await terminate(process)
                state.children.discard(process)
    if op == "model_start":
        state.metrics()
        model = p["model"]
        config = p["configuration"]
        if state.model == model and state.process and state.process.poll() is None:
            return {"running": True, "model": model}
        metrics = await asyncio.to_thread(state.gpu.claim, model, int(config["estimated_vram_mb"]), 768)
        state.model = model
        try:
            from huggingface_hub import snapshot_download
            path = await asyncio.to_thread(snapshot_download, config["model_id"], revision=config["revision"], local_files_only=True)
            fraction = min(.85, (metrics["free_mb"] - 768) / metrics["total_mb"])
            args = [sys.executable, "-m", "vllm.entrypoints.openai.api_server", "--model", path,
                    "--served-model-name", model, "--host", "127.0.0.1", "--port", "8100",
                    "--dtype", config.get("dtype", "half"), "--max-model-len", str(config.get("max_model_len", 2048)),
                    "--gpu-memory-utilization", str(fraction), "--max-num-seqs", "1", "--enforce-eager"]
            if config.get("quantization"):
                args += ["--quantization", config["quantization"]]
            state.log = (directory / "vllm.log").open("wb")
            state.process = subprocess.Popen(args, stdout=state.log, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + int(os.getenv("TF_VLLM_START_TIMEOUT", "300"))
            while time.monotonic() < deadline:
                if state.process.poll() is not None:
                    raise RuntimeError("vLLM startup failed: " + (directory / "vllm.log").read_text(errors="replace")[-4000:])
                try:
                    response = await state.http.get("http://127.0.0.1:8100/health", timeout=2)
                    if response.status_code == 200:
                        return {"model": model, "revision": config["revision"], "gpu_memory_utilization": fraction,
                                "startup_seconds": time.monotonic() - started}
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(1)
            raise RuntimeError("vLLM startup timeout")
        except BaseException:
            await state.stop()
            raise
    if op == "ingest":
        raw = base64.b64decode(p["content"], validate=True)
        if len(raw) > 10 << 20:
            raise ValueError("Upload exceeds 10 MiB")
        if p["filename"].lower().endswith(".pdf"):
            def parse():
                from pypdf import PdfReader
                reader = PdfReader(io.BytesIO(raw))
                if len(reader.pages) > 1000:
                    raise ValueError("PDF exceeds 1000 pages")
                return "\n".join(page.extract_text() or "" for page in reader.pages)
            text = await asyncio.to_thread(parse)
        else:
            text = raw.decode("utf-8")
        chunks = chunk_text(text)
        result = []
        embedding_id = ""
        for start in range(0, len(chunks), 32):
            batch = chunks[start:start + 32]
            embedded = await state.embeddings(batch)
            embedding_id = embedded["embedding_id"]
            result.extend({"text": t, "embedding": v} for t, v in zip(batch, embedded["vectors"], strict=True))
        return {"embedding_id": embedding_id, "chunks": result, "sha256": hashlib.sha256(raw).hexdigest()}
    if op not in {"train", "evaluate", "benchmark"}:
        raise ValueError(f"Unsupported operation: {op}")
    state.metrics()
    required = 1024 if op == "benchmark" else int(p["model_config"]["estimated_vram_mb"]) + (2048 if op == "train" else 768)
    await asyncio.to_thread(state.gpu.claim, body.id, required, 768)
    process = None
    try:
        manifest = {"id": body.id, "operation": op, "payload": p, "data_dir": str(state.data)}
        (directory / "manifest.json").write_text(json.dumps(manifest, indent=2))
        with (directory / "worker.log").open("wb") as log:
            process = subprocess.Popen([sys.executable, "-m", "tensorforge.experiments", str(directory / "manifest.json")],
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            state.children.add(process)
            while process.poll() is None:
                await asyncio.sleep(.25)
            if process.returncode != 0:
                raise RuntimeError((directory / "worker.log").read_text(errors="replace")[-6000:])
        result = json.loads((directory / "result.json").read_text())
        result["artifacts"] = sorted(str(path.relative_to(directory)) for path in directory.rglob("*")
                                     if path.is_file() and path.name != "manifest.json")
        return result
    finally:
        if process:
            await terminate(process)
            state.children.discard(process)
        state.gpu.release(body.id)


# No mutable management state is kept here; only local model processes and CUDA leases.
app = create_app()
