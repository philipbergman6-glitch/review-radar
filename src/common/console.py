"""Console behaviour every entrypoint wants, in the one place that knows the exceptions."""
from __future__ import annotations

import sys


def line_buffered_stdout() -> None:
    """Flush prints as they happen, where the stream supports it.

    Two reasons this is not just `print(..., flush=True)` everywhere. Redirected to a file,
    stdout is block-buffered, so a process the exactly-once gate SIGKILLs loses everything
    since the last 4 KB boundary and its log ends mid-startup (audit F7). And a long Spark job
    watched live should print as it goes rather than in 4 KB bursts.

    The guard is what makes this importable from a notebook: a kernel's stdout is an ipykernel
    `OutStream`, which has no `reconfigure` and needs none -- it forwards every write. Without
    the guard, importing any job module into the demo notebook raises `AttributeError` before a
    single cell runs (ADR-0009: the notebook calls the pipeline's own code path, so the
    pipeline's own modules have to survive being imported there).
    """
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if reconfigure is not None:
        reconfigure(line_buffering=True)
