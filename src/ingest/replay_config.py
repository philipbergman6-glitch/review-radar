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
import re
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT

CONFIG_PATH = PROJECT_ROOT / "conf" / "stream_replay.toml"

#: The one watermark spelling this project uses. Spark accepts a whole interval grammar;
#: accepting all of it here would mean the loader could not compare the delay against the two
#: injected lags, which is the check that makes the slice counts falsifiable.
WATERMARK_GRAMMAR = re.compile(r"^(\d+) days?$")


def watermark_days(text: str) -> int:
    """`"30 days"` -> 30. Anything else is a hard failure, not a default."""
    m = WATERMARK_GRAMMAR.match(str(text).strip())
    if m is None:
        raise ValueError(f"stream.watermark must read '<n> days', got {text!r}: the delay is "
                         "compared against the injected lags in days, so a unit this loader "
                         "cannot compare is refused rather than passed through to Spark")
    return int(m.group(1))


@dataclass(frozen=True)
class ReplayConfig:
    status: str
    spec_version: str
    topic: str
    sample_topic: str
    demo_topic: str
    partitions: int
    tiebreak: str
    suffix: str
    records_per_second: int
    clock_every_records: int
    watermark: str
    max_offsets_per_trigger: int
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

    @property
    def watermark_days(self) -> int:
        return watermark_days(self.watermark)

    def topic_for(self, scope: str, run_kind: str = "control") -> str:
        """`full` -> the stream topic, `sample` -> its own; the demo run has its own again.

        Sample and full never share a topic (ADR-0008), and the control and demo runs never
        do either: the lineage gate resolves a replay by its topic's record count, so two
        replays on one topic would leave the count belonging to neither (ticket 16). The demo
        run is a full-scope run only -- the frozen slices cannot be drawn from the sample.
        """
        if scope not in ("full", "sample"):
            raise ValueError(f"scope must be 'full' or 'sample', got {scope!r}")
        if run_kind not in ("control", "demo"):
            raise ValueError(f"run_kind must be 'control' or 'demo', got {run_kind!r}")
        if run_kind == "demo":
            if scope != "full":
                raise ValueError("the demo run is full-scope only: the frozen slices are drawn "
                                 "from the whole file and the sample cannot hold them")
            return self.demo_topic
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
    stream = doc["stream"]

    names = (topic["name"], topic["sample_name"], topic["demo_name"])
    if len(set(names)) != len(names):
        raise ValueError("topic.name, topic.sample_name and topic.demo_name must all differ: "
                         "two replays on one topic leave its record count belonging to "
                         "neither (ADR-0008, ticket 16)")
    if int(topic["partitions"]) != 1:
        raise ValueError(
            f"topic.partitions must be 1, got {topic['partitions']}. Kafka orders records "
            "within a partition and nowhere else, so a multi-partition stream topic does not "
            "carry the sort job's event-time order and the watermark would drop rows the "
            "partitioning reordered rather than rows that were actually late (ticket 15)")
    if sort["tiebreak"] != "review_id+line_sha256":
        raise ValueError(
            f"sort.tiebreak must be 'review_id+line_sha256', got {sort['tiebreak']!r}. "
            "review_id alone is not unique in this source (key collisions), so the line "
            "digest is not optional.")
    for key, value in (("pacing.records_per_second", pacing["records_per_second"]),
                       ("pacing.clock_every_records", pacing["clock_every_records"]),
                       ("topic.partitions", topic["partitions"]),
                       ("slices.near_rows", sl["near_rows"]),
                       ("slices.far_rows", sl["far_rows"]),
                       ("stream.max_offsets_per_trigger", stream["max_offsets_per_trigger"])):
        if int(value) < 1:
            raise ValueError(f"{key} must be >= 1, got {value}")
    if int(sl["near_lag_days"]) >= int(sl["far_lag_days"]):
        raise ValueError("slices.near_lag_days must be shorter than slices.far_lag_days")
    if sl["near_salt"] == sl["far_salt"]:
        raise ValueError("the two slices must draw under different salts, or they overlap")
    if not watermark_days(stream["watermark"]) > int(sl["near_lag_days"]) \
            or not watermark_days(stream["watermark"]) < int(sl["far_lag_days"]):
        raise ValueError(
            f"stream.watermark {stream['watermark']!r} does not separate the two slices: it "
            f"must be longer than slices.near_lag_days ({sl['near_lag_days']}) so the near "
            f"slice is accepted and shorter than slices.far_lag_days ({sl['far_lag_days']}) so "
            "the far slice is dropped. A watermark that does not sit between them makes the "
            "injected counts unfalsifiable (ADR-0010)")

    config_hash = hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()
    return ReplayConfig(
        status=status, spec_version=str(doc["protocol"]["spec_version"]),
        topic=topic["name"], sample_topic=topic["sample_name"],
        demo_topic=topic["demo_name"], partitions=int(topic["partitions"]),
        tiebreak=sort["tiebreak"], suffix=sort["suffix"],
        records_per_second=int(pacing["records_per_second"]),
        clock_every_records=int(pacing["clock_every_records"]),
        watermark=stream["watermark"],
        max_offsets_per_trigger=int(stream["max_offsets_per_trigger"]),
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
