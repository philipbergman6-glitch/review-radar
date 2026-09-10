"""The audit set is opened once, and the seal is the evidence of it (ADR-0002, ticket 09).

P6's whole protocol exists to protect one measurement: three systems -- the LLM labeller, the
MLlib classifier and the star-only floor -- scored on held-out reviews that none of them was
tuned against. Everything tunable was frozen first: the prompt (ticket 06), the classifier's
per-theme cuts (ticket 08), the star thresholds (RR-23), the taxonomy, and the pass rule
itself. After the pass, none of them moves.

"None of them moves" is the kind of claim a repo makes and cannot later demonstrate. This
module makes it checkable, in two halves:

* **Before.** `fingerprint` hashes every frozen thing the number depends on. The pass records
  it in the seal, alongside the material it was computed from, *before* any audit row is read.
* **After.** `freezes_moved` re-derives the fingerprint from today's config files and returns
  named differences. `scripts/gate_themes.py` prints them, so a threshold edited the day after
  the audit fails the gate instead of passing quietly under a better number.

And one refusal in the middle: `require_audit_unopened` means the pass cannot run twice. A
second pass is not a repeat of the first -- it is a second look at held-out data, which is the
thing being ruled out. Re-running measurement after seeing a result is how a holdout becomes a
development set without anyone deciding that it should.

**Inference is not opening the set.** Labelling the audit reviews, or running the classifier
over them, produces predictions; that can be re-run, resumed after a crash, and is idempotent
by key. What may happen only once is *scoring* -- turning those predictions into a number
against the reference labels. The three scorers therefore refuse `--sample audit` outright and
name the one command that may do it (`require_measurement_goes_through_the_pass`).

Pure: json, hashlib, no pyspark, so the pass, the gate and the tests read one rule.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from src.ai.classifier_spec import (
    ClassifierSpec,
    SystemIdentity,
    classifier_identity,
    llm_identity,
    load_classifier_spec,
)
from src.ai.labels import LabelSpec, Taxonomy, load_spec, load_taxonomy, prompt_schema
from src.common.config import PROJECT_ROOT

SAMPLE = "audit"
SEAL_PATH = PROJECT_ROOT / "eval" / "themes" / "audit-seal.json"
STAR_SPEC_PATH = PROJECT_ROOT / "conf" / "theme-star-baseline.json"
STAR_LABEL_SOURCE = "star_only"

#: ADR-0003's pass rule, frozen 2026-09-06 and unmoved through RR-19, RR-21 and ticket 06.
#: It lives here rather than in the gate that prints it because it is *inside* the fingerprint:
#: a bar lowered after the audit invalidates the measurement exactly as a refitted cut does,
#: and a rule the seal cannot see is a rule the seal cannot protect.
MACRO_F1_BAR = 0.70
MIN_RECALL_BAR = 0.50

#: The system whose number P6 publishes. The other two are the comparison ADR-0002 requires.
PUBLISHED_SYSTEM = "llm"
SYSTEMS = ("llm", "classifier", STAR_LABEL_SOURCE)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def artefact_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ------------------------------------------------------------------- the star floor ----
def load_star_spec(path: Path = STAR_SPEC_PATH) -> dict[str, Any]:
    if not path.exists():
        raise ValueError(f"{path.name} is missing; fit the star-only floor on development "
                         "first (make star-baseline-fit, RR-23)")
    doc = json.loads(path.read_text())
    if doc.get("fitted_on") != "development":
        raise ValueError(f"the star thresholds were fitted on {doc.get('fitted_on')!r}, not on "
                         "'development'; a floor fitted on the set it is scored against is not "
                         "a floor")
    return doc


def star_identity(doc: dict[str, Any]) -> SystemIdentity:
    """The floor has no inference and no config hash: it is a rating and a frozen threshold."""
    return SystemIdentity(label_source=STAR_LABEL_SOURCE, model_id=STAR_LABEL_SOURCE,
                          prompt_version=f"star-baseline-v{doc['version']}",
                          inference_config_hash="", artefact_stem=STAR_LABEL_SOURCE)


# --------------------------------------------------------------------- the freezes ----
def frozen_llm_identity(spec: LabelSpec, tax: Taxonomy) -> SystemIdentity:
    """The labeller under the frozen prompt -- derived, never passed in.

    The pass, the seal and the gate must all name the same rows. A prompt handed in by an
    operator is a prompt that can be handed in wrong, and the failure is silent: a different
    config hash selects no rows, and "no rows" reads like "this was never run".
    """
    frozen = spec.require_frozen(purpose="opening the audit set")
    return llm_identity(spec, prompt_name=frozen.name, model_id=spec.model_id,
                        taxonomy_hash=tax.file_hash,
                        schema=prompt_schema(frozen.version, tax.ids))


def collect_freezes(spec: LabelSpec | None = None, tax: Taxonomy | None = None,
                    clf: ClassifierSpec | None = None,
                    star: dict[str, Any] | None = None) -> dict[str, Any]:
    """Every frozen thing the audit numbers depend on, as one comparable structure.

    Raises rather than returning a partial answer: a fingerprint over "the two freezes that
    happened to exist" would be a fingerprint that quietly changes meaning when the third
    lands. The arguments exist so a test can substitute a spec without writing config files.
    """
    spec = spec if spec is not None else load_spec()
    tax = tax if tax is not None else load_taxonomy()
    clf = clf if clf is not None else load_classifier_spec()
    star = star if star is not None else load_star_spec()

    frozen = spec.require_frozen(purpose="opening the audit set")
    llm = frozen_llm_identity(spec, tax)
    if clf.thresholds is None:
        raise ValueError("the classifier has no frozen cuts; the audit set cannot be opened "
                         "for a system that has not finished being fitted (make "
                         "classifier-thresholds, ticket 08)")
    if clf.thresholds_fitted_on != "development":
        raise ValueError(f"the classifier's cuts were fitted on {clf.thresholds_fitted_on!r}, "
                         "not on 'development'")
    for name, hashed in (("classifier", clf.taxonomy_hash), ("star_only", star["taxonomy_hash"])):
        if hashed != tax.file_hash:
            raise ValueError(f"the {name} system was fitted against taxonomy {hashed[:12]}, but "
                             f"the taxonomy is {tax.file_hash[:12]} today; half of what a theme "
                             "label means is the taxonomy, so these are not comparable systems")
    return {
        "taxonomy": {"version": tax.version, "hash": tax.file_hash, "themes": len(tax.themes)},
        "llm": {"prompt_name": frozen.name, "prompt_version": frozen.version,
                "freeze_commit": frozen.freeze_commit, "decided_in": frozen.decided_in,
                "model_id": spec.model_id, "label_spec_version": spec.label_spec_version,
                "inference_config_hash": llm.inference_config_hash},
        "classifier": {"spec_version": clf.spec_version, "spec_hash": clf.spec_hash,
                       "training_labels_config": clf.training_labels_config,
                       "thresholds_fitted_on": clf.thresholds_fitted_on,
                       "thresholds": {k: clf.thresholds[k] for k in sorted(clf.thresholds)}},
        "star_only": {"version": str(star["version"]), "fitted_on": star["fitted_on"],
                      "thresholds": {k: int(v) for k, v in sorted(star["thresholds"].items())}},
        "pass_rule": {"macro_f1_bar": MACRO_F1_BAR, "min_supported_recall_bar": MIN_RECALL_BAR},
    }


def fingerprint(freezes: dict[str, Any]) -> str:
    return _sha256(json.dumps(freezes, sort_keys=True, ensure_ascii=False))


def _flatten(node: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(node, dict):
        out: dict[str, Any] = {}
        for k, v in node.items():
            out.update(_flatten(v, f"{prefix}.{k}" if prefix else str(k)))
        return out
    return {prefix: node}


def freezes_moved(sealed: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """Named differences between what the audit was measured under and what is frozen today.

    Named, not a boolean: "the fingerprint differs" sends a reader to diff two JSON blobs,
    while "classifier.thresholds.delivery 0.41 -> 0.37" says what happened. The seal stores
    the whole freeze material for exactly this reason -- a hash can only ever detect.
    """
    was, now = _flatten(sealed), _flatten(current)
    diffs = [f"{k} {was[k]!r} -> {now[k]!r}" for k in sorted(was) if k in now and was[k] != now[k]]
    diffs += [f"{k} was {was[k]!r}, absent today" for k in sorted(set(was) - set(now))]
    diffs += [f"{k} is {now[k]!r}, absent at the audit" for k in sorted(set(now) - set(was))]
    return diffs


# ------------------------------------------------------------------------ the seal ----
def load_seal(path: Path = SEAL_PATH) -> dict[str, Any] | None:
    return json.loads(path.read_text()) if path.exists() else None


def require_audit_unopened(path: Path = SEAL_PATH) -> None:
    """The audit set is scored once. A second pass is refused, with the first one's evidence."""
    seal = load_seal(path)
    if seal is None:
        return
    scored = ", ".join(f"{s['system']} macro_f1="
                       f"{'none' if s['macro_f1'] is None else round(s['macro_f1'], 4)}"
                       for s in seal["systems"])
    raise ValueError(
        f"the audit set was already opened on {seal['opened_at']} at commit "
        f"{seal.get('git_commit_sha', '')[:8]} ({scored}). It is scored once: a second pass "
        "after a result has been seen is a second look at held-out data, whatever it is "
        f"called. The numbers are in {path.name} and the artefacts it names; nothing about "
        "them is re-derived by measuring again.")


def require_measurement_goes_through_the_pass(sample: str, *, command: str) -> None:
    """`--sample audit` on a single-system scorer: refused, naming the one command that may.

    Scoring one system at a time is how the audit set gets opened three times by accident --
    and the third opening is the one that follows two disappointing numbers. `make audit-once`
    scores all three together or none of them.
    """
    if sample == SAMPLE:
        raise ValueError(
            f"{command} will not score the {SAMPLE} set. All three systems are scored in one "
            "pass, so that none of them is measured after another's result is known: run "
            "`make audit-once` (ticket 09). Inference over the audit reviews is not this "
            "refusal -- labelling and classifier prediction may run whenever they need to.")


def build_seal(*, freezes: dict[str, Any], systems: list[dict[str, Any]], scope: str,
               reference_rows: int, git_commit_sha: str, opened_at: str) -> dict[str, Any]:
    """The record written at the moment of opening. Verdict included, however it reads.

    A macro-F1 below the bar is a reported FAIL, not a blocker (ticket 09): P6 becomes
    `built, evaluated, below target` and P7 is not held up. The verdict is therefore written
    into the seal rather than left to the reader, because the alternative -- deciding what a
    disappointing number means after seeing it -- is the failure this whole file guards.
    """
    published = next((s for s in systems if s["system"] == PUBLISHED_SYSTEM), None)
    if published is None:
        raise ValueError(f"the seal must carry the published system {PUBLISHED_SYSTEM!r}")
    if {s["system"] for s in systems} != set(SYSTEMS):
        raise ValueError(f"the audit pass scores exactly {SYSTEMS}, got "
                         f"{sorted(s['system'] for s in systems)}")
    m, r = published["macro_f1"], published["min_supported_recall"]
    return {
        "sample": SAMPLE, "scope": scope, "opened_at": opened_at,
        "git_commit_sha": git_commit_sha,
        "freeze_fingerprint": fingerprint(freezes), "freezes": freezes,
        "reference_rows": reference_rows, "systems": systems,
        "pass_rule": {"macro_f1_bar": MACRO_F1_BAR,
                      "min_supported_recall_bar": MIN_RECALL_BAR,
                      "published_system": PUBLISHED_SYSTEM},
        "verdict": {
            "macro_f1": m, "min_supported_recall": r,
            "passed": m is not None and m >= MACRO_F1_BAR
                      and r is not None and r >= MIN_RECALL_BAR,
            "note": "a miss is reported, not re-tuned: P6 is then `built, evaluated, below "
                    "target` and P7 is not held up (ticket 09)"},
    }


def seal_matches_artefacts(seal: dict[str, Any], *, scores_dir: Path) -> list[str]:
    """Named failures if a score file the seal points at is missing or has changed since.

    The seal records each artefact's sha256 because the artefacts are plain files in the repo:
    the fingerprint proves the *inputs* did not move, and this proves the *outputs* did not.
    """
    fails: list[str] = []
    for s in seal["systems"]:
        path = scores_dir / s["artefact"]
        if not path.exists():
            fails.append(f"{s['system']}: {s['artefact']} is named by the seal but not on disk")
        elif artefact_sha256(path) != s["artefact_sha256"]:
            fails.append(f"{s['system']}: {s['artefact']} has changed since the audit was sealed")
    return fails


def identities() -> dict[str, SystemIdentity]:
    """The three systems' identities, derived from the frozen configs alone."""
    spec, tax = load_spec(), load_taxonomy()
    return {"llm": frozen_llm_identity(spec, tax),
            "classifier": classifier_identity(load_classifier_spec()),
            STAR_LABEL_SOURCE: star_identity(load_star_spec())}
