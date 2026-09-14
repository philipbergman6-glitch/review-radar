"""The frozen replay protocol: `conf/stream_replay.toml` read, validated and hashed.

One loader for both P8 producers (the sort job and the paced replay) so that neither can
run under a rate or a slice size the other did not see. The whole document is hashed, not a
subset: unlike the decline rule there is no "execution setting" here that may drift without
changing what a run means -- the topic, the rate, the lags and the slice sizes are all part
of the claim the stream gate makes.

Hard-fails on a protocol that is still provisional. ADR-0010's numbers are only evidence if
they were committed before the run they describe, and a loader that shrugged at
`status = "provisional"` would let a rate be tuned against a run and then frozen afterwards,
which is the one thing the freeze exists to prevent.
"""
from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT

CONFIG_PATH = PROJECT_ROOT / "conf" / "stream_replay.toml"


@dataclass(frozen=True)
class ReplayConfig:
    status: str
    spec_version: str
    topic: str
    sample_topic: str
    partitions: int
    tiebreak: str
    suffix: str
    records_per_second: int
    clock_every_records: int
    seed: int
    near_lag_days: int
    near_rows: int
    near_salt: str
    far_lag_days: int
    far_rows: int
    far_salt: str
    config_hash: str
    path: Path

    @property
    def frozen(self) -> bool:
        return self.status == "frozen"

    def topic_for(self, scope: str) -> str:
        """`full` -> the stream topic, `sample` -> its own. They never share (ADR-0008)."""
        if scope not in ("full", "sample"):
            raise ValueError(f"scope must be 'full' or 'sample', got {scope!r}")
        return self.topic if scope == "full" else self.sample_topic

    def as_params(self) -> dict[str, Any]:
        d = asdict(self)
        d["path"] = str(self.path.relative_to(PROJECT_ROOT))
        return d


def load_replay_config(path: Path = CONFIG_PATH) -> ReplayConfig:
    with path.open("rb") as f:
        doc = tomllib.load(f)

    status = doc["protocol"]["status"]
    if status not in ("provisional", "frozen"):
        raise ValueError(f"protocol.status must be provisional|frozen, got {status!r}")
    if status != "frozen":
        raise RuntimeError(
            f"{path} is still provisional. Pacing and slice sizes are committed before the "
            "first run (ADR-0010); freeze the protocol, then replay.")

    topic, sort, pacing, sl = doc["topic"], doc["sort"], doc["pacing"], doc["slices"]

    if topic["name"] == topic["sample_name"]:
        raise ValueError("topic.name and topic.sample_name must differ (ADR-0008)")
    if sort["tiebreak"] != "review_id+line_sha256":
        raise ValueError(
            f"sort.tiebreak must be 'review_id+line_sha256', got {sort['tiebreak']!r}. "
            "review_id alone is not unique in this source (key collisions), so the line "
            "digest is not optional.")
    for key, value in (("pacing.records_per_second", pacing["records_per_second"]),
                       ("pacing.clock_every_records", pacing["clock_every_records"]),
                       ("topic.partitions", topic["partitions"]),
                       ("slices.near_rows", sl["near_rows"]),
                       ("slices.far_rows", sl["far_rows"])):
        if int(value) < 1:
            raise ValueError(f"{key} must be >= 1, got {value}")
    if int(sl["near_lag_days"]) >= int(sl["far_lag_days"]):
        raise ValueError("slices.near_lag_days must be shorter than slices.far_lag_days")
    if sl["near_salt"] == sl["far_salt"]:
        raise ValueError("the two slices must draw under different salts, or they overlap")

    config_hash = hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()
    return ReplayConfig(
        status=status, spec_version=str(doc["protocol"]["spec_version"]),
        topic=topic["name"], sample_topic=topic["sample_name"],
        partitions=int(topic["partitions"]),
        tiebreak=sort["tiebreak"], suffix=sort["suffix"],
        records_per_second=int(pacing["records_per_second"]),
        clock_every_records=int(pacing["clock_every_records"]),
        seed=int(sl["seed"]),
        near_lag_days=int(sl["near_lag_days"]), near_rows=int(sl["near_rows"]),
        near_salt=sl["near_salt"],
        far_lag_days=int(sl["far_lag_days"]), far_rows=int(sl["far_rows"]),
        far_salt=sl["far_salt"],
        config_hash=config_hash, path=path)


def slice_rank(seed: int, salt: str, review_id: str) -> bytes:
    """The deterministic draw key for a held-back slice.

    Same construction as the theme sampling draw (`src/gold/controls.py`): a hash of
    (seed, salt, id) with 0x1f separators, ranked ascending. A slice is therefore a pure
    function of the frozen config and the file's content -- its expected size is known
    before a producer starts, which is what lets the gate assert a drop count instead of
    reporting one.
    """
    return hashlib.sha256(f"{seed}\x1f{salt}\x1f{review_id}".encode()).digest()
