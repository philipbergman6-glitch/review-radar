"""MiniLM encoder bound to the embedding spec; one process-wide model instance.

`encode` returns unit-length float32 vectors (the spec says normalize=true, the ES field is
cosine). The model revision is pinned from the spec so a silent upstream update cannot
change a vector under the same hash.
"""
from __future__ import annotations

from typing import Any

import numpy as np

from src.ai.spec import EmbeddingSpec, load_spec

_MODEL: Any = None
_MODEL_HASH: str | None = None


def model(spec: EmbeddingSpec | None = None):
    global _MODEL, _MODEL_HASH
    spec = spec or load_spec()
    if _MODEL is None or _MODEL_HASH != spec.hash:
        from sentence_transformers import SentenceTransformer  # slow import, on demand
        m = SentenceTransformer(spec.model, revision=spec.identity["revision"],
                                device=spec.execution.get("device", "cpu"))
        m.max_seq_length = int(spec.identity["max_seq_length"])
        dim = (m.get_embedding_dimension() if hasattr(m, "get_embedding_dimension")
               else m.get_sentence_embedding_dimension())
        if dim != spec.dims:
            raise RuntimeError(f"model dimension {dim} != spec dims {spec.dims}")
        _MODEL, _MODEL_HASH = m, spec.hash
    return _MODEL


def encode(texts: list[str], *, spec: EmbeddingSpec | None = None, batch_size: int | None = None) -> np.ndarray:
    spec = spec or load_spec()
    bs = batch_size or int(spec.execution.get("batch_size", 64))
    vecs = model(spec).encode(texts, batch_size=bs, normalize_embeddings=True,
                              convert_to_numpy=True, show_progress_bar=False)
    out = np.asarray(vecs, dtype=np.float32)
    if out.shape != (len(texts), spec.dims):
        raise RuntimeError(f"encode returned shape {out.shape}, expected ({len(texts)}, {spec.dims})")
    return out


def encode_query(query: str, spec: EmbeddingSpec | None = None) -> list[float]:
    return encode([query], spec=spec)[0].tolist()
