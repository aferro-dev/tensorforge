import os
os.environ.setdefault("TF_SERVICE_KEY", "test-internal-service-key-at-least-32-characters")
os.environ.setdefault("TF_DATA_DIR", "/tmp/ai-test")

import base64
import asyncio
from pathlib import Path
import pytest
from fastapi.testclient import TestClient
from tensorforge.ai import AIState, Operation, chunk_text, create_app, run_operation, safe_dir
from tensorforge.experiments import text_scores


class FakeState(AIState):
    async def embeddings(self, texts):
        return {"vectors": [[1.0] + [0.0]*383 for _ in texts], "embedding_id": "test-fixture-only"}


def test_chunks_preserve_tail_and_overlap():
    text = " ".join(f"word{i}" for i in range(1000))
    chunks = chunk_text(text)
    assert len(chunks) > 1
    assert chunks[0].startswith("word0")
    assert chunks[-1].endswith("word999")
    assert all(len(c) <= 1400 for c in chunks)
    assert chunks[0][-200:] == chunks[1][:200]
    with pytest.raises(ValueError):
        chunk_text("   ")


def test_artifact_path_is_bounded(tmp_path):
    with pytest.raises(ValueError):
        safe_dir(tmp_path, "../../etc")
    assert safe_dir(tmp_path, "a"*24).parent == tmp_path / "jobs"


def test_internal_auth_and_gpu_failure():
    with TestClient(create_app()) as client:
        assert client.get("/gpu").status_code == 401
        response = client.get("/gpu", headers={"Authorization": "Bearer "+os.environ["TF_SERVICE_KEY"]})
        assert response.status_code == 200
        assert "available" in response.json()


def test_ingestion_records_real_provenance(tmp_path):
    state = FakeState()
    state.data = tmp_path
    body = Operation(id="b"*24, operation="ingest", payload={"filename":"example.md", "content":base64.b64encode(b"A documented fact.").decode()})
    out = asyncio.run(run_operation(state, body))
    assert out["embedding_id"] == "test-fixture-only"
    assert out["chunks"][0]["text"] == "A documented fact."
    assert len(out["sha256"]) == 64
    asyncio.run(state.http.aclose())


def test_missing_gpu_fails_without_results(tmp_path):
    state = AIState()
    state.data = tmp_path
    class MissingGPU:
        def metrics(self):
            return {"available": False}
        def claim(self, *args):
            raise RuntimeError("NVIDIA GPU unavailable")
    state.gpu = MissingGPU()
    with pytest.raises(RuntimeError, match="GPU unavailable"):
        asyncio.run(run_operation(state, Operation(id="c"*24, operation="benchmark", payload={"size":1024})))
    assert not (tmp_path/"jobs"/("c"*24)/"result.json").exists()
    asyncio.run(state.http.aclose())


def test_evaluation_metrics():
    assert text_scores("a b", "a b") == {"exact_match":1.0,"token_f1":1.0}
    assert text_scores("a a", "a b")["token_f1"] == .5
    assert text_scores("x", "y")["exact_match"] == 0


def test_artifacts_cannot_escape_job_or_expose_manifest(tmp_path):
    state = FakeState()
    state.data = tmp_path
    job = "d"*24
    directory = safe_dir(tmp_path, job)
    (directory/"manifest.json").write_text('{"private":"training corpus"}')
    (directory/"result.json").write_text('{"metrics":{}}')
    headers = {"Authorization":"Bearer "+os.environ["TF_SERVICE_KEY"]}
    with TestClient(create_app(state)) as client:
        assert client.get(f"/artifacts/{job}",params={"path":"manifest.json"},headers=headers).status_code == 404
        assert client.get(f"/artifacts/{job}",params={"path":"../../secret"},headers=headers).status_code == 404
        result = client.get(f"/artifacts/{job}",params={"path":"result.json"},headers=headers)
        assert result.status_code == 200
        assert result.json() == {"metrics":{}}
