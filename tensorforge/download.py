"""Cancellable Hugging Face snapshot downloader; invoked only by the AI service."""
import json
from pathlib import Path
import re
import sys


def main():
    from huggingface_hub import snapshot_download
    repository, revision, output = sys.argv[1:]
    if not re.fullmatch(r"[a-fA-F0-9]{40}", revision):
        raise ValueError("An immutable Hugging Face revision is required")
    snapshot = snapshot_download(repository, revision=revision)
    Path(output).write_text(json.dumps({"snapshot": snapshot, "revision": revision, "repository": repository}))


if __name__ == "__main__":
    main()
