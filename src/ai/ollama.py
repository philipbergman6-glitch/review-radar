"""The local inference client: one logical inference, at most two attempts (ADR-0003, RR-19).

Ollama constrains decoding to the JSON schema we pass, which removes most parse failures but
guarantees nothing semantic: the model can still quote text that is not in the review. So a
response is validated twice -- once as JSON against the schema by the server, once by the
caller's semantic validator here -- and a failure is retried exactly once with *identical*
inputs. We never strip fences, repair JSON, coerce a value or manufacture a quote: after the
second failure the attempt list is stored with both raw responses and both validation errors,
and the row is terminal `parse_failed` (bad output) or `api_failed` (transport).

Everything a run learns about an attempt -- the raw response, the validation error, token
counts, duration -- is returned, because ADR-0003 requires every label to be auditable.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from src.ai.labels import LabelSpec


@dataclass
class Attempt:
    attempt_no: int
    raw_response: str | None
    validation_error: str | None
    input_tokens: int | None
    output_tokens: int | None
    duration_s: float
    completed_at: float


@dataclass
class Inference:
    status: str                      # succeeded | parse_failed | api_failed
    parsed: dict[str, Any] | None
    attempts: list[Attempt] = field(default_factory=list)

    @property
    def attempt_count(self) -> int:
        return len(self.attempts)


class OllamaError(RuntimeError):
    pass


def generate(spec: LabelSpec, *, system: str, prompt: str, schema: dict[str, Any],
             model_id: str | None = None, timeout: float = 300.0) -> tuple[str, dict[str, Any]]:
    """One HTTP call. Returns (response text, server metrics). Raises OllamaError on transport."""
    inf = spec.inference
    body = {
        "model": model_id or spec.model_id,
        "system": system,
        "prompt": prompt,
        "stream": False,
        "format": schema,
        "think": bool(inf.get("think", False)),
        "options": {"temperature": float(inf["temperature"]),
                    "num_predict": int(inf["num_predict"]),
                    "seed": int(inf["seed"])},
    }
    req = urllib.request.Request(f"{spec.endpoint}/api/generate",
                                 json.dumps(body).encode(), {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        raise OllamaError(f"{type(exc).__name__}: {exc}") from exc
    return payload.get("response", ""), payload


def infer(spec: LabelSpec, *, system: str, prompt: str, schema: dict[str, Any],
          validate: Callable[[Any], list[str]], model_id: str | None = None,
          timeout: float = 300.0) -> Inference:
    """One logical inference: call, validate, retry once identically, then give up terminally."""
    max_attempts = int(spec.limits.get("max_attempts", 2))
    attempts: list[Attempt] = []
    last_status = "api_failed"
    for n in range(1, max_attempts + 1):
        t0 = time.time()
        try:
            raw, meta = generate(spec, system=system, prompt=prompt, schema=schema,
                                 model_id=model_id, timeout=timeout)
        except OllamaError as exc:
            attempts.append(Attempt(n, None, f"api: {exc}", None, None,
                                    round(time.time() - t0, 3), time.time()))
            last_status = "api_failed"
            continue
        dt = round(time.time() - t0, 3)
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as exc:
            attempts.append(Attempt(n, raw, f"json: {exc}", meta.get("prompt_eval_count"),
                                    meta.get("eval_count"), dt, time.time()))
            last_status = "parse_failed"
            continue
        fails = validate(obj)
        attempts.append(Attempt(n, raw, "; ".join(fails) if fails else None,
                                meta.get("prompt_eval_count"), meta.get("eval_count"), dt, time.time()))
        if not fails:
            return Inference("succeeded", obj, attempts)
        last_status = "parse_failed"
    return Inference(last_status, None, attempts)
