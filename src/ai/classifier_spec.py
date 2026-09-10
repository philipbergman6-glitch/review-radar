"""How each scored system is picked out of `gold.review_theme_labels` (ADR-0002, ticket 08).

Three systems answer the same question on the same reviews, and their rows share one table.
What separates them is an *identity*: `(label_source, model_id, prompt_version,
inference_config_hash)`. The labeller's identity is a prompt and a model under one inference
config; the classifier's is the spec hash of the fitted model, which covers its hyperparameters,
the taxonomy and the config hash of the labels that taught it.

Resolving that identity is not a detail of a script. A scorer that looked for classifier rows
under the *labeller's* config hash would find none and report a score over nothing, so the
resolution is one function per system, tested, and the artefact filename comes from the same
place -- `scripts/gate_themes.py` finds the audit score by rebuilding its stem.

The two freezes ADR-0002 asks for are also here, as refusals rather than conventions:

* `require_frozen_cuts` -- a classifier-score cut is fitted on development, once. Cuts fitted
  anywhere else, or against another taxonomy, are refused at scoring time.
* `require_development_first` -- the audit set is not scored before the development number is
  on disk. Ticket 08 commits development; ticket 09 opens audit. Reversing that order would
  make "we fitted once and scored once" unverifiable after the fact.

Pure: json, dataclasses, no pyspark, so the Spark job, the scorer and the tests read one rule.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.ai.labels import LABEL_SOURCES, LabelSpec
from src.common.config import PROJECT_ROOT

SPEC_PATH = PROJECT_ROOT / "conf" / "theme-classifier.json"
SCORES_DIR = PROJECT_ROOT / "eval" / "themes"
LABEL_SOURCE = "classifier"
#: The primary implementation, whose rows are the only ones primary analysis may read (ADR-0002).
LLM_LABEL_SOURCE = "local_llm"
#: Not a model *file*: the family of the fitted model, as the labels table records it.
MODEL_ID = "mllib_logreg"
FIT_FRAME = "development"

for _source in (LABEL_SOURCE, LLM_LABEL_SOURCE):
    if _source not in LABEL_SOURCES:
        raise ValueError(f"{_source!r} is not one of the frozen label sources {LABEL_SOURCES}")


@dataclass(frozen=True)
class SystemIdentity:
    """The four columns that pick one system's rows out of the shared labels table."""

    label_source: str
    model_id: str
    prompt_version: str
    inference_config_hash: str
    artefact_stem: str


@dataclass(frozen=True)
class ClassifierSpec:
    spec_version: str
    spec_hash: str
    model_path: str
    taxonomy_hash: str
    training_labels_config: str
    thresholds: dict[str, float] | None
    thresholds_fitted_on: str | None
    raw: dict[str, Any]

    @property
    def prompt_version(self) -> str:
        """The classifier has no prompt; it fills that column with its own spec version."""
        return f"classifier-v{self.spec_version}"


def parse_classifier_spec(doc: dict[str, Any]) -> ClassifierSpec:
    """Validate `conf/theme-classifier.json` as structure.

    A spec with no `thresholds` is legal and means "trained, not yet fitted": `--train` writes
    the file and `--fit-thresholds` fills it in. A spec with *partial* cuts is not legal -- a
    theme with no cut would silently predict nothing.
    """
    cuts = doc.get("thresholds")
    if cuts is not None:
        from src.ai.labels import load_taxonomy

        expected = set(load_taxonomy().ids)
        if set(cuts) != expected:
            missing = sorted(expected - set(cuts)) or sorted(set(cuts) - expected)
            raise ValueError(f"the classifier spec has no cut for {missing}; a theme without a "
                             "cut predicts nothing and would score as a silent zero")
        for theme, cut in cuts.items():
            if not 0.0 < float(cut) < 1.0:
                raise ValueError(f"the cut for {theme} is {cut}, which is not a classifier "
                                 "score in (0, 1)")
        cuts = {t: float(c) for t, c in cuts.items()}
    return ClassifierSpec(
        spec_version=str(doc["spec_version"]), spec_hash=doc["spec_hash"],
        model_path=doc["model_path"], taxonomy_hash=doc["taxonomy_hash"],
        training_labels_config=doc["training_labels_config"], thresholds=cuts,
        thresholds_fitted_on=doc.get("thresholds_fitted_on"), raw=doc)


def load_classifier_spec(path: Path = SPEC_PATH) -> ClassifierSpec:
    if not path.exists():
        raise ValueError(f"{path.name} is missing; train the classifier first (make classifier-train)")
    return parse_classifier_spec(json.loads(path.read_text()))


# --------------------------------------------------------------- the identities ----
def llm_artefact_stem(*, prompt_version: str, model_id: str) -> str:
    """`<prompt version>-<model>`, with the colon a filename cannot carry replaced.

    Two callers depend on this being one function: `scripts/score_themes.py` writes the file
    and `scripts/gate_themes.py` rebuilds the name to read it. A drift between them shows up
    as `artefact=missing` and a FAIL over a score that exists.
    """
    return f"{prompt_version}-{model_id.replace(':', '_')}"


def score_artefact_name(*, sample: str, stem: str) -> str:
    return f"score-{sample}-{stem}.json"


def classifier_identity(spec: ClassifierSpec) -> SystemIdentity:
    """The fitted classifier. Its stem carries the spec hash, because a refit is a new system.

    Without the hash, a retrained classifier would write over the previous one's score file and
    `require_development_first` would then be satisfied by a number measured on a model that no
    longer exists -- the freeze it is there to demonstrate, defeated by a filename.
    """
    return SystemIdentity(label_source=LABEL_SOURCE, model_id=MODEL_ID,
                          prompt_version=spec.prompt_version,
                          inference_config_hash=spec.spec_hash,
                          artefact_stem=f"{spec.prompt_version}-{spec.spec_hash[:12]}")


def llm_identity(spec: LabelSpec, *, prompt_name: str, model_id: str, schema: dict[str, Any],
                 taxonomy_hash: str) -> SystemIdentity:
    """The labeller under one prompt and model. The stem is `<prompt version>-<model>`."""
    prompt = spec.prompts[prompt_name]
    return SystemIdentity(
        label_source=LLM_LABEL_SOURCE, model_id=model_id, prompt_version=prompt.version,
        inference_config_hash=spec.config_hash(prompt_name, schema,
                                               extra={"taxonomy": taxonomy_hash},
                                               model_id=model_id),
        artefact_stem=llm_artefact_stem(prompt_version=prompt.version, model_id=model_id))


# ------------------------------------------------------------------ the two freezes ----
def require_same_taxonomy(spec: ClassifierSpec, *, taxonomy_hash: str) -> None:
    """Half of what a theme label means is the taxonomy; a fit under another one is not this one."""
    if spec.taxonomy_hash != taxonomy_hash:
        raise ValueError("the classifier was fitted against a different taxonomy "
                         f"({spec.taxonomy_hash[:12]} != {taxonomy_hash[:12]})")


def require_frozen_cuts(spec: ClassifierSpec, *, taxonomy_hash: str) -> dict[str, float]:
    """The cuts, or a refusal naming which of ADR-0002's conditions the spec fails."""
    if not spec.thresholds:
        raise ValueError("the classifier has no frozen cuts; run `make classifier-thresholds` "
                         "to fit them on development first")
    if spec.thresholds_fitted_on != FIT_FRAME:
        raise ValueError(f"the cuts were fitted on {spec.thresholds_fitted_on!r}, not on "
                         f"{FIT_FRAME!r}; a cut fitted anywhere else is tuned against a set it "
                         "is then scored on")
    require_same_taxonomy(spec, taxonomy_hash=taxonomy_hash)
    return spec.thresholds


def require_frozen_labeller(given: str | None, *, frozen_config_hash: str) -> str:
    """The pool's config hash: the frozen labeller's, or a refusal naming the difference.

    `--source-config-hash` is how the operator says which labels the classifier learned from,
    and a hash is exactly the kind of argument that gets pasted one prompt version out of date.
    The rows differ only by that column, so a wrong one trains quietly on labels from a
    superseded prompt and ADR-0002's comparison against the labeller's own numbers stops
    meaning anything.
    """
    if given and given != frozen_config_hash:
        raise ValueError(f"--source-config-hash {given[:12]} is not the frozen prompt's "
                         f"({frozen_config_hash[:12]}); ADR-0002 compares the classifier against "
                         "the LLM theme labeller under one prompt, so the LLM-labelled training "
                         "pool must be the one that prompt wrote")
    return frozen_config_hash


def development_score_path(identity: SystemIdentity, *, scores_dir: Path = SCORES_DIR) -> Path:
    return scores_dir / score_artefact_name(sample=FIT_FRAME, stem=identity.artefact_stem)


def require_development_first(sample: str, *, identity: SystemIdentity,
                              scores_dir: Path = SCORES_DIR) -> None:
    """Audit is opened once, and only after the development number exists (ADR-0002, ticket 09).

    The lookup lives here rather than at the call sites: two commands can open an audit number
    -- the scoring job and the scorer that tabulates it -- and a guard restated twice is a guard
    that will one day be restated only once.
    """
    if sample != "audit":
        return
    path = development_score_path(identity, scores_dir=scores_dir)
    if not path.exists():
        raise ValueError(f"{path.name} is not on disk, so this system has no committed "
                         "development score; opening the audit set now would leave the one "
                         "fitting pass unverifiable (score development first, ticket 08)")
