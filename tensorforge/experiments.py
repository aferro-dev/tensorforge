"""Isolated GPU worker: real LoRA training, paired evaluation and Triton benchmarks."""
import collections
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys
import time


def environment():
    packages = {}
    for name in ["torch", "triton", "transformers", "peft", "accelerate", "vllm"]:
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    import torch
    try:
        driver = subprocess.check_output(["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"], text=True).strip()
    except (OSError, subprocess.SubprocessError):
        driver = "unavailable"
    return {"python": sys.version, "platform": platform.platform(), "packages": packages,
            "cuda": torch.version.cuda, "cudnn": torch.backends.cudnn.version(), "driver": driver,
            "device": torch.cuda.get_device_name(0), "capability": list(torch.cuda.get_device_capability(0)),
            "total_vram_bytes": torch.cuda.get_device_properties(0).total_memory}


def seed_all(seed):
    import numpy as np
    import torch
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.benchmark = False
    torch.use_deterministic_algorithms(True)


def emit(directory, event):
    event["timestamp"] = time.time()
    with (directory / "events.jsonl").open("a") as f:
        f.write(json.dumps(event) + "\n")


def load_model(config, training=False):
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer
    if config.get("quantization"):
        raise ValueError("LoRA/evaluation runner currently requires an unquantized base model; quantized inference remains supported")
    kwargs = {"revision": config["revision"], "trust_remote_code": False, "local_files_only": True}
    tokenizer = AutoTokenizer.from_pretrained(config["model_id"], **kwargs)
    if tokenizer.pad_token_id is None:
        tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(config["model_id"], torch_dtype=torch.float16,
                                                attn_implementation="eager", **kwargs).to("cuda")
    model.config.use_cache = not training
    return model, tokenizer


def checkpoint(directory, model, optimizer, scaler, step, losses, metadata):
    import torch
    temp = directory / f"checkpoint-{step}.partial"
    temp.mkdir()
    model.save_pretrained(temp / "adapter")
    torch.save({"optimizer": optimizer.state_dict(), "scaler": scaler.state_dict(), "step": step,
                "losses": losses, "python_rng": random.getstate(), "torch_rng": torch.get_rng_state(),
                "cuda_rng": torch.cuda.get_rng_state_all(), "metadata": metadata}, temp / "state.pt")
    temp.rename(directory / f"checkpoint-{step}")
    emit(directory, {"type": "checkpoint", "step": step, "path": f"checkpoint-{step}"})


def train(p, directory, data_dir):
    import torch
    from peft import LoraConfig, PeftModel, get_peft_model
    model, tokenizer = load_model(p["model_config"], training=True)
    model.enable_input_require_grads()
    model.gradient_checkpointing_enable(gradient_checkpointing_kwargs={"use_reentrant": False})
    resume = None
    if p.get("resume_job"):
        prior = data_dir / "jobs" / p["resume_job"]
        candidates = [path for path in prior.glob("checkpoint-*") if not path.name.endswith(".partial")]
        if not candidates:
            raise ValueError("No complete checkpoint available to resume")
        resume = max(candidates, key=lambda x: int(x.name.split("-")[-1]))
        model = PeftModel.from_pretrained(model, resume / "adapter", is_trainable=True)
    else:
        model = get_peft_model(model, LoraConfig(r=int(p["rank"]), lora_alpha=2*int(p["rank"]),
                                               target_modules="all-linear", task_type="CAUSAL_LM", lora_dropout=0))
    # Keep trainable adapters in fp32 for stable mixed-precision optimization.
    for parameter in model.parameters():
        if parameter.requires_grad:
            parameter.data = parameter.data.float()
    optimizer = torch.optim.AdamW((x for x in model.parameters() if x.requires_grad), lr=float(p["learning_rate"]))
    scaler = torch.amp.GradScaler("cuda")
    losses = []
    start_step = 0
    metadata = {"model": p["model_config"], "dataset_version": p["dataset_version"], "seed": p["seed"],
                "rank": p["rank"], "max_length": p["max_length"], "learning_rate": p["learning_rate"]}
    if resume:
        # Only files produced by this service in a private volume are loaded.
        saved = torch.load(resume / "state.pt", weights_only=False, map_location="cpu")
        if saved["metadata"] != metadata:
            raise ValueError("Checkpoint metadata mismatch")
        optimizer.load_state_dict(saved["optimizer"])
        scaler.load_state_dict(saved["scaler"])
        start_step, losses = saved["step"], saved["losses"]
        random.setstate(saved["python_rng"])
        torch.set_rng_state(saved["torch_rng"])
        torch.cuda.set_rng_state_all(saved["cuda_rng"])
    if start_step >= int(p["steps"]):
        raise ValueError("Requested total steps must exceed checkpoint step")
    records = p["records"]
    model.train()
    started = time.monotonic()
    for step in range(start_step, int(p["steps"])):
        record = records[step % len(records)]
        text = record.get("text") or record["prompt"] + "\n" + record["completion"]
        batch = tokenizer(text, return_tensors="pt", truncation=True, max_length=int(p["max_length"]))
        if batch["input_ids"].shape[-1] < 2:
            raise ValueError(f"Record {step % len(records)} has fewer than 2 tokens")
        batch = {k: v.to("cuda") for k, v in batch.items()}
        optimizer.zero_grad(set_to_none=True)
        with torch.autocast("cuda", dtype=torch.float16):
            loss = model(**batch, labels=batch["input_ids"]).loss
        if not torch.isfinite(loss):
            raise ValueError("Nonfinite training loss")
        scaler.scale(loss).backward()
        scaler.unscale_(optimizer)
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        scaler.step(optimizer)
        scaler.update()
        losses.append(float(loss.detach()))
        emit(directory, {"type": "train", "step": step+1, "loss": losses[-1],
                         "runtime_seconds": time.monotonic()-started,
                         "allocated_bytes": torch.cuda.memory_allocated(),
                         "peak_bytes": torch.cuda.max_memory_allocated()})
        if (step+1) % int(p["checkpoint_steps"]) == 0 or step+1 == int(p["steps"]):
            checkpoint(directory, model, optimizer, scaler, step+1, losses, metadata)
    model.save_pretrained(directory / "adapter")
    tokenizer.save_pretrained(directory / "tokenizer")
    return {"losses": losses, "steps": int(p["steps"]), "runtime_seconds": time.monotonic()-started,
            "peak_allocated_bytes": torch.cuda.max_memory_allocated(), "metadata": metadata,
            "effective_training": {"dtype": "float16", "batch_size": 1, "optimizer": "AdamW",
                                   "gradient_checkpointing": True, "adapter_dtype": "float32"},
            "adapter": str((directory / "adapter").relative_to(data_dir))}


def text_scores(prediction, expected):
    pred = prediction.lower().split()
    gold = expected.lower().split()
    overlap = sum((collections.Counter(pred) & collections.Counter(gold)).values())
    f1 = 2*overlap/(len(pred)+len(gold)) if pred or gold else 1.0
    return {"exact_match": float(prediction.strip() == expected.strip()), "token_f1": f1}


def evaluate(p, directory, data_dir):
    import torch
    from peft import PeftModel
    model, tokenizer = load_model(p["model_config"])
    adapter = data_dir / "jobs" / p["adapter_job"] / "adapter"
    if not adapter.is_dir():
        raise ValueError("Adapter artifact missing")
    model = PeftModel.from_pretrained(model, adapter)
    model.eval()
    results = []
    for i, record in enumerate(p["records"]):
        prompt = tokenizer(record["prompt"], return_tensors="pt", truncation=True, max_length=1024)
        prompt = {k: v.to("cuda") for k, v in prompt.items()}
        outputs = {}
        for mode in ["baseline", "adapter"]:
            seed_all(int(p["seed"]))
            with torch.inference_mode():
                if mode == "baseline":
                    with model.disable_adapter():
                        generated = model.generate(**prompt, do_sample=False, max_new_tokens=int(p["max_new_tokens"]), pad_token_id=tokenizer.pad_token_id)
                else:
                    generated = model.generate(**prompt, do_sample=False, max_new_tokens=int(p["max_new_tokens"]), pad_token_id=tokenizer.pad_token_id)
            prediction = tokenizer.decode(generated[0, prompt["input_ids"].shape[-1]:], skip_special_tokens=True)
            outputs[mode] = {"prediction": prediction, **text_scores(prediction, record["completion"])}
        results.append({"index": i, "prompt": record["prompt"], "expected": record["completion"], **outputs})
        emit(directory, {"type": "evaluation", "completed": i+1, "total": len(p["records"])})
    aggregates = {mode: {metric: statistics.mean(row[mode][metric] for row in results)
                        for metric in ["exact_match", "token_f1"]} for mode in ["baseline", "adapter"]}
    (directory / "predictions.json").write_text(json.dumps(results, indent=2))
    return {"metrics": aggregates, "sample_count": len(results), "dataset_version": p["dataset_version"],
            "model": p["model_config"], "adapter_job": p["adapter_job"], "seed": p["seed"],
            "generation": {"do_sample": False, "max_new_tokens": p["max_new_tokens"]}}


def benchmark(p, directory):
    import torch
    import triton
    import triton.language as tl

    @triton.jit
    def add_kernel(x, y, output, n: tl.constexpr, BLOCK: tl.constexpr):
        offsets = tl.program_id(0) * BLOCK + tl.arange(0, BLOCK)
        mask = offsets < n
        tl.store(output + offsets, tl.load(x + offsets, mask=mask) + tl.load(y + offsets, mask=mask), mask=mask)

    n = int(p["size"])
    dtype = getattr(torch, p["dtype"])
    x = torch.randn(n, device="cuda", dtype=dtype)
    y = torch.randn_like(x)
    baseline = torch.empty_like(x)
    custom = torch.empty_like(x)
    def pytorch():
        torch.add(x, y, out=baseline)
    def triton_call():
        add_kernel[(triton.cdiv(n, 256),)](x, y, custom, n, BLOCK=256)
    pytorch()
    triton_call()
    torch.cuda.synchronize()
    tolerance = 1e-3 if dtype == torch.float16 else 1e-6
    torch.testing.assert_close(custom, baseline, rtol=tolerance, atol=tolerance)
    # Also cover tail masking with non-block-aligned sizes and small tensors.
    for count in [1, 255, 257, 1025]:

        a = torch.randn(count, device="cuda", dtype=dtype)
        b = torch.randn_like(a)
        out = torch.empty_like(a)
        add_kernel[(triton.cdiv(a.numel(), 256),)](a, b, out, a.numel(), BLOCK=256)
        torch.testing.assert_close(out, a+b, rtol=tolerance, atol=tolerance)
    for _ in range(int(p["warmup"])):
        pytorch()
        triton_call()
    torch.cuda.synchronize()
    samples = {"pytorch": [], "triton": []}
    functions = {"pytorch": pytorch, "triton": triton_call}
    for repetition in range(int(p["repetitions"])):
        order = ["pytorch", "triton"] if repetition % 2 == 0 else ["triton", "pytorch"]
        for name in order:
            start, end = torch.cuda.Event(enable_timing=True), torch.cuda.Event(enable_timing=True)
            start.record()
            functions[name]()
            end.record()
            end.synchronize()
            samples[name].append(start.elapsed_time(end))
    medians = {key: statistics.median(values) for key, values in samples.items()}
    source = Path(__file__).read_bytes()
    return {"kernel": "vector_add", "shape": [n], "dtype": p["dtype"], "warmup": p["warmup"],
            "repetitions": p["repetitions"], "seed": p["seed"], "raw_latency_ms": samples,
            "median_latency_ms": medians, "speedup": medians["pytorch"]/medians["triton"],
            "correctness": {"passed": True, "rtol": tolerance, "atol": tolerance,
                            "max_absolute_error": float((custom-baseline).abs().max()), "tail_cases": [1, 255, 257, 1025]},
            "method": "CUDA events; preallocated outputs; alternating order; excludes allocation and JIT warmup",
            "source_sha256": hashlib.sha256(source).hexdigest()}


def main():
    os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"
    manifest = json.loads(Path(sys.argv[1]).read_text())
    directory = Path(sys.argv[1]).parent
    import torch
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA unavailable: a real NVIDIA GPU and compatible container runtime are required")
    total = torch.cuda.get_device_properties(0).total_memory
    free, _ = torch.cuda.mem_get_info()
    budget = min(.85, (free-768*2**20)/total)
    if budget <= 0:
        raise RuntimeError("No VRAM available after safety reserve")
    torch.cuda.set_per_process_memory_fraction(budget)
    p = manifest["payload"]
    seed_all(int(p["seed"]))
    op = manifest["operation"]
    emit(directory, {"type": "started", "operation": op, "environment": environment()})
    if op == "train":
        result = train(p, directory, Path(manifest["data_dir"]))
    elif op == "evaluate":
        result = evaluate(p, directory, Path(manifest["data_dir"]))
    elif op == "benchmark":
        result = benchmark(p, directory)
    else:
        raise ValueError("Unknown GPU operation")
    result["environment"] = environment()
    result["source_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (directory / "result.json").write_text(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
