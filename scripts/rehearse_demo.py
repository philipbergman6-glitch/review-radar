"""Run one rehearsal: execute the demo notebook and write the recorded backup beside it.

A rehearsal is an *executed export* (ADR-0009, RR-11 §4), so this script does not report that
the demo went well -- it produces the evidence `scripts/gate_demo.py` reads back and can
disagree with:

  docs/demo/<date>/demo.executed.ipynb   the executed notebook, cell timings recorded
  docs/demo/<date>/demo.html             the same notebook rendered; the playable backup
  docs/demo/<date>/kibana-dashboard.png  the dashboard move 5 switches to, screenshotted
  docs/demo/<date>/exactly-once.txt      copied from the stand-in at docs/demo/

The HTML is converted **from the executed notebook**, never executed a second time, so the two
files are one run seen twice -- which is what lets the gate tie them together by the git SHA
and the `DEMO_LIVE` line they must share.

The dashboard PNG is taken here, by pointing headless Chrome at the same URL move 5 switches
to -- Kibana's own PNG export is a licensed feature this stack does not have. The exactly-once
transcript is copied from `docs/demo/exactly-once.txt`, because `make eos` kills a job
mid-stream and no rehearsal should be doing that on the way past; make it once with

  make eos | tee docs/demo/exactly-once.txt

Either file can also simply be dropped at `docs/demo/<name>` by hand, and is copied in.

Move 2 resets the topic it replays and the notebook stops the replay part-way, so a rehearsal
leaves it half-written. That topic is `reviews.stream.demo` (ADR-0010): the control run the
lineage and stream gates re-read is untouched, and a rehearsal costs nothing to run again.

Run:  ./run.sh python scripts/rehearse_demo.py [--scope full] [--timeout 900]
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from src.common import config as C
from src.gates import demo as gate
from src.serving import demo as stage

NOTEBOOK = C.PROJECT_ROOT / "notebooks" / "demo.ipynb"
DEMO_ROOT = C.PROJECT_ROOT / "docs" / "demo"

#: Copied in rather than produced: `make eos` kills a job mid-stream to prove a point, and a
#: rehearsal has no business doing that on the way past. Make it once, it is the same evidence
#: every time. A hand-made `docs/demo/kibana-dashboard.png` is copied the same way when the
#: screenshot below cannot be taken.
STAND_INS = {
    gate.EXPORT_TRANSCRIPT: "make eos | tee docs/demo/exactly-once.txt",
    gate.EXPORT_KIBANA: "screenshot the dashboard at $(make demo-run-sheet) move 5 into docs/demo/",
}

#: Headless Chrome, because Kibana's own PNG export is a licensed feature this stack does not
#: have. `CHROME` overrides the path; the default is where macOS puts it.
CHROME = os.getenv("CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")

#: Long enough for the dashboard's three panels to query Elasticsearch and draw.
RENDER_BUDGET_MS = 30_000


def export_dir(root: Path = DEMO_ROOT, *, now: datetime | None = None) -> Path:
    """`docs/demo/<date>`, suffixed when the day already holds a rehearsal.

    The second rehearsal of a day is a second directory, not an overwrite: "run clean twice"
    is two exports, and a runner that reused the directory could only ever produce one.
    """
    day = (now or datetime.now(UTC)).strftime("%Y-%m-%d")
    candidate, n = root / day, 1
    while candidate.exists():
        n += 1
        candidate = root / f"{day}-{n}"
    return candidate


def execute(out: Path, *, timeout: int) -> None:
    """`nbconvert --execute` into the export directory, with cell timings recorded.

    `--allow-errors` on purpose: a cell that raised must still reach disk, because an export
    carrying a failed cell is the evidence the gate rejects. Without it nbconvert writes
    nothing and a broken rehearsal would be indistinguishable from one nobody ran.
    """
    subprocess.run(
        ["./run.sh", "python", "-m", "jupyter", "nbconvert", "--to", "notebook", "--execute",
         "--allow-errors",
         "--ExecutePreprocessor.timeout", str(timeout),
         "--ExecutePreprocessor.record_timing", "True",
         "--output-dir", str(out), "--output", gate.EXPORT_NOTEBOOK, str(NOTEBOOK)],
        cwd=C.PROJECT_ROOT, check=False)


def render(out: Path) -> None:
    """Render the executed notebook. No `--execute`: this is the same run, seen twice."""
    subprocess.run(
        ["./run.sh", "python", "-m", "jupyter", "nbconvert", "--to", "html",
         "--output-dir", str(out), "--output", gate.EXPORT_HTML,
         str(out / gate.EXPORT_NOTEBOOK)],
        cwd=C.PROJECT_ROOT, check=True)


def screenshot_kibana(out: Path, *, url: str, chrome: str = CHROME) -> bool:
    """Point headless Chrome at the dashboard move 5 shows. False when it could not.

    `--virtual-time-budget` rather than a sleep: Chrome fast-forwards its own clock until the
    page stops working, so the panels are drawn when the shot is taken instead of being raced.
    """
    if not Path(chrome).exists():
        return False
    # Its own profile and working directory: Chrome scatters lock files and crash dumps
    # wherever it is started, and the export directory is a committed artefact.
    with tempfile.TemporaryDirectory() as scratch:
        done = subprocess.run(
            [chrome, "--headless=new", "--disable-gpu", "--no-sandbox", "--hide-scrollbars",
             f"--user-data-dir={scratch}", "--window-size=1600,1200",
             f"--virtual-time-budget={RENDER_BUDGET_MS}",
             f"--screenshot={out / gate.EXPORT_KIBANA}", url],
            cwd=scratch, capture_output=True, text=True, check=False)
    if not (out / gate.EXPORT_KIBANA).exists():
        print(f"REHEARSAL_KIBANA chrome wrote no screenshot: {done.stderr.strip()[-300:]}",
              file=sys.stderr)
        return False
    return True


def copy_stand_ins(out: Path, root: Path = DEMO_ROOT) -> list[str]:
    """Copy the hand-made files in, skipping any the rehearsal already produced.

    Returns the names that are neither produced nor there to copy -- the gate will refuse the
    backup for exactly those, and naming them here is cheaper than reading it off the gate.
    """
    missing = []
    for name in STAND_INS:
        if (out / name).exists():
            continue
        src = root / name
        if src.exists():
            shutil.copyfile(src, out / name)
        else:
            missing.append(name)
    return missing


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--timeout", type=int, default=900,
                    help="per-cell execution timeout in seconds (not the rehearsal's own "
                         "ceiling, which the gate measures from the recorded cell timings)")
    args = ap.parse_args()
    if args.scope != "full":
        # A rehearsal at sample scope rehearses a different demo (ADR-0011: a phase is complete
        # only at full scope), so it is refused rather than exported and later discounted.
        sys.exit("a rehearsal runs the demo as it will be given, at --scope full")

    out = export_dir()
    out.mkdir(parents=True, exist_ok=True)
    print(f"REHEARSAL_DIR {out.relative_to(C.PROJECT_ROOT)}")
    execute(out, timeout=args.timeout)
    if not (out / gate.EXPORT_NOTEBOOK).exists():
        sys.exit(f"nbconvert wrote no notebook into {out}; nothing to rehearse against")
    render(out)
    if not screenshot_kibana(out, url=stage.kibana_url()):
        print("REHEARSAL_KIBANA no headless Chrome at $CHROME; falling back to the stand-in",
              file=sys.stderr)
    for name in copy_stand_ins(out):
        print(f"REHEARSAL_MISSING {name} -- make it once with: {STAND_INS[name]}",
              file=sys.stderr)
    print(f"REHEARSAL_EXPORTED {out.relative_to(C.PROJECT_ROOT)} "
          f"files={','.join(sorted(p.name for p in out.iterdir()))}")
    print("REHEARSAL_GATE run `make gate-demo` to count this export against the threshold")


if __name__ == "__main__":
    main()
