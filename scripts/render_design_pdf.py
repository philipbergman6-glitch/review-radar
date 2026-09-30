"""Render docs/DESIGN.md to docs/DESIGN.pdf and fail if it runs past two pages.

The brief asks for a one to two page design document with an architecture diagram.
GitHub renders the Mermaid block; a PDF reader does not, so this goes through pandoc to
HTML, lets mermaid.js draw the diagram in headless Chrome, and prints A4. The page count
is read back from the PDF itself: a design doc that grew a third page exits non-zero.

Needs `pandoc`, `pdfinfo` (poppler), Google Chrome, and network access for mermaid.js.
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "docs" / "DESIGN.md"
CSS = ROOT / "docs" / "assets" / "design-print.css"
TARGET = ROOT / "docs" / "DESIGN.pdf"
CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
MAX_PAGES = 2

#: pandoc emits the fenced block as <pre class="mermaid"><code>; mermaid.js wants a bare div.
MERMAID = """
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<script>
document.querySelectorAll("pre.mermaid code").forEach(c => {
  const d = document.createElement("div");
  d.className = "mermaid";
  d.textContent = c.textContent;
  c.parentElement.replaceWith(d);
});
mermaid.initialize({startOnLoad: true, theme: "neutral"});
</script>
"""


def main() -> int:
    for tool in ("pandoc", "pdfinfo"):
        if shutil.which(tool) is None:
            raise SystemExit(f"{tool} is not on PATH")
    if not CHROME.exists():
        raise SystemExit(f"Chrome not found at {CHROME}")

    with tempfile.TemporaryDirectory() as tmp:
        after = Path(tmp) / "mermaid.html"
        after.write_text(MERMAID, encoding="utf-8")
        html = Path(tmp) / "design.html"
        subprocess.run(
            ["pandoc", str(SOURCE), "-f", "gfm", "-t", "html5", "-s",
             "--metadata", "pagetitle=Review Radar — design document",
             "-c", CSS.as_uri(), "-A", str(after), "-o", str(html)],
            check=True,
        )
        subprocess.run(
            [str(CHROME), "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
             "--virtual-time-budget=8000", f"--print-to-pdf={TARGET}", html.as_uri()],
            check=True, capture_output=True,
        )

    info = subprocess.run(["pdfinfo", str(TARGET)], check=True, capture_output=True, text=True).stdout
    pages = int(re.search(r"^Pages:\s+(\d+)", info, re.MULTILINE).group(1))
    verdict = "OK" if pages <= MAX_PAGES else "FAIL"
    print(f"DESIGN_PDF pages={pages} max={MAX_PAGES} path={TARGET.relative_to(ROOT)} DESIGN_PDF={verdict}")
    return 0 if pages <= MAX_PAGES else 1


if __name__ == "__main__":
    sys.exit(main())
