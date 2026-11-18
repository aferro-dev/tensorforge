import threading


class GPU:
    def __init__(self):
        self.lock = threading.Lock()
        self.active = None

    def metrics(self):
        try:
            import pynvml as nv
            nv.nvmlInit()
            try:
                h = nv.nvmlDeviceGetHandleByIndex(0)
                mem = nv.nvmlDeviceGetMemoryInfo(h)
                util = nv.nvmlDeviceGetUtilizationRates(h)
                name = nv.nvmlDeviceGetName(h)
                return {"available": True, "device": name, "total_mb": mem.total // 2**20,
                        "used_mb": mem.used // 2**20, "free_mb": mem.free // 2**20,
                        "utilization_percent": util.gpu, "active_workload": self.active,
                        "memory_source": "NVML device used memory (includes all processes)"}
            finally:
                nv.nvmlShutdown()
        except Exception as exc:
            return {"available": False, "error": str(exc), "active_workload": self.active}

    def claim(self, workload, required_mb, reserve_mb):
        with self.lock:
            if self.active:
                raise RuntimeError(f"GPU reserved by {self.active}")
            metrics = self.metrics()
            if not metrics["available"]:
                raise RuntimeError(f"NVIDIA GPU unavailable: {metrics['error']}")
            if required_mb + reserve_mb > metrics["free_mb"]:
                raise RuntimeError(f"Insufficient VRAM: need {required_mb}+{reserve_mb} MiB reserve; "
                                   f"free {metrics['free_mb']} MiB")
            self.active = workload
            return metrics

    def release(self, workload):
        with self.lock:
            if self.active == workload:
                self.active = None
