"""Pure verdict functions -- one module per gated capability (ticket 02).

Every gate in this project is split at the same seam. The script does the I/O: it reads the
run ledger, the pinned Iceberg snapshots, Elasticsearch, the frozen specs and the judgement
files, and hands the resulting facts to a function here. That function is pure -- it formats
the gate's named constituent lines, decides the terminal `<NAME>_GATE=PASS|FAIL`, and returns
an `evaluation.Verdict` carrying both, plus the metric its evaluation artefact publishes.

Nothing in this package imports pyspark, elasticsearch, psycopg or the network, so every
verdict is unit-testable with no services running -- which is the point: a gate's decision
logic is the part worth testing, and it used to be reachable only by standing up the stack.
"""
