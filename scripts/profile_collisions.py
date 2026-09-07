"""Collision anatomy of the raw review file (RR-02, 2026-09-07).

Groups rows by (user_id, parent_asin, timestamp), classifies each group as exact or
conflicting over the seven retained fields, and prints the counts that ADR-0007 cites.
Pure Python over the raw JSONL, so it shares no code with silver.

Run:  ./run.sh python scripts/profile_collisions.py [path-to-reviews.jsonl]
"""
import json
import statistics as stats
import sys
from collections import Counter, defaultdict

PATH = sys.argv[1] if len(sys.argv) > 1 else "data/raw/All_Beauty.jsonl"

FIELDS = ["rating", "title", "text", "verified_purchase", "helpful_vote", "asin", "images"]

rows = []  # store minimal needed fields + line number
with open(PATH, "r") as f:
    for i, line in enumerate(f):
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        rows.append({
            "lineno": i,  # 0-indexed; will report as-is, note base
            "user_id": d.get("user_id"),
            "parent_asin": d.get("parent_asin"),
            "timestamp": d.get("timestamp"),
            "rating": d.get("rating"),
            "title": d.get("title"),
            "text": d.get("text"),
            "verified_purchase": d.get("verified_purchase"),
            "helpful_vote": d.get("helpful_vote"),
            "asin": d.get("asin"),
            "images": d.get("images"),
        })

total_rows = len(rows)
print(f"Total rows read: {total_rows}")

# Empty text count overall
def is_empty_text(t):
    return t is None or (isinstance(t, str) and t.strip() == "")

empty_text_overall = sum(1 for r in rows if is_empty_text(r["text"]))
print(f"[1] Empty-text rows overall: {empty_text_overall}")

# ---- Task 1: key = (user_id, parent_asin, timestamp)
key_groups = defaultdict(list)
for r in rows:
    key = (r["user_id"], r["parent_asin"], r["timestamp"])
    key_groups[key].append(r)

collision_groups = {k: v for k, v in key_groups.items() if len(v) > 1}
num_collision_groups = len(collision_groups)
total_rows_in_collisions = sum(len(v) for v in collision_groups.values())
rows_removed_if_dedup = total_rows_in_collisions - num_collision_groups

print("\n=== TASK 1: (user_id, parent_asin, timestamp) collisions ===")
print(f"[observed] Number of collision groups (>1 row): {num_collision_groups}")
print(f"[observed] Total rows in those groups: {total_rows_in_collisions}")
print(f"[observed] Rows removed if 1 survivor/group: {rows_removed_if_dedup}")

# ---- Task 2: group-size distribution
size_dist = Counter(len(v) for v in collision_groups.values())
print("\n=== TASK 2: group-size distribution ===")
for size in sorted(size_dist):
    print(f"  size={size}: {size_dist[size]} groups")
size2 = size_dist.get(2, 0)
size3 = size_dist.get(3, 0)
size4plus = sum(c for s, c in size_dist.items() if s >= 4)
print(f"[observed] size=2: {size2}, size=3: {size3}, size>=4: {size4plus}")

# ---- Task 3 & 4: classify groups EXACT vs CONFLICTING; which fields differ
def field_values(group, field):
    vals = []
    for r in group:
        v = r[field]
        if field == "images":
            # normalize list for comparison (order-sensitive first; could also try set)
            v = json.dumps(v, sort_keys=True) if v is not None else None
        vals.append(v)
    return vals

exact_count = 0
conflicting_count = 0
field_diff_counter = Counter()
group_diff_field_sets = []  # for classification of task 4

for key, group in collision_groups.items():
    diffs = []
    for field in FIELDS:
        vals = field_values(group, field)
        if len(set(vals)) > 1:
            diffs.append(field)
    if not diffs:
        exact_count += 1
    else:
        conflicting_count += 1
        for f in diffs:
            field_diff_counter[f] += 1
        group_diff_field_sets.append(set(diffs))

print("\n=== TASK 3: EXACT vs CONFLICTING classification ===")
print(f"[observed] EXACT groups: {exact_count}")
print(f"[observed] CONFLICTING groups: {conflicting_count}")
print("[observed] Field-diff counts among conflicting groups (a group can count in multiple fields):")
for f in FIELDS:
    print(f"  {f}: {field_diff_counter.get(f, 0)}")

# ---- Task 4: subsets
only_asin = sum(1 for s in group_diff_field_sets if s == {"asin"})
only_helpful_vote = sum(1 for s in group_diff_field_sets if s == {"helpful_vote"})
text_or_rating = sum(1 for s in group_diff_field_sets if ("text" in s or "rating" in s))

print("\n=== TASK 4: conflicting-group subsets ===")
print(f"[observed] Differ ONLY in asin: {only_asin}")
print(f"[observed] Differ ONLY in helpful_vote: {only_helpful_vote}")
print(f"[observed] Differ in text and/or rating: {text_or_rating}")

# ---- Task 5: adjacency of collision rows (line-number gaps)
all_within_1 = 0
gaps = []
first_line_is_min_survivor_deterministic = True  # trivially true if we pick min(lineno)
for key, group in collision_groups.items():
    linenos = sorted(r["lineno"] for r in group)
    max_gap = linenos[-1] - linenos[0]
    span_ok = all(linenos[i+1] - linenos[i] <= 1 for i in range(len(linenos)-1))
    if span_ok:
        all_within_1 += 1
    gaps.append(max_gap)

median_gap = stats.median(gaps)
print("\n=== TASK 5: adjacency of collision rows ===")
print(f"[observed] Groups with all rows within 1 line of each other (contiguous): {all_within_1} / {num_collision_groups}")
print(f"[observed] Median max-line-gap within group: {median_gap}")
print("[observed] Line numbers are 0-indexed (0 = first line of file).")
print("[observed] 'First line in file' (min lineno) survivor selection: deterministic by construction (min() is always well-defined and unique per group), since line numbers are unique per row.")

# ---- Task 6: empty text among collision rows
empty_text_collisions = sum(1 for v in collision_groups.values() for r in v if is_empty_text(r["text"]))
print("\n=== TASK 6: empty text ===")
print(f"[observed] Empty-text rows overall: {empty_text_overall}")
print(f"[observed] Empty-text rows among collision-group rows: {empty_text_collisions}")

# ---- Task 7: (user_id, parent_asin) collisions ignoring timestamp
key_groups2 = defaultdict(list)
for r in rows:
    key = (r["user_id"], r["parent_asin"])
    key_groups2[key].append(r)

collision_groups2 = {k: v for k, v in key_groups2.items() if len(v) > 1}
num_collision_groups2 = len(collision_groups2)
total_rows_in_collisions2 = sum(len(v) for v in collision_groups2.values())
rows_removed2 = total_rows_in_collisions2 - num_collision_groups2

print("\n=== TASK 7: (user_id, parent_asin) collisions (ignoring timestamp) ===")
print(f"[observed] Number of collision groups: {num_collision_groups2}")
print(f"[observed] Total rows in those groups: {total_rows_in_collisions2}")
print(f"[observed] Rows removed if 1 survivor/group: {rows_removed2}")

# ---- Task 8: distinct asin -> multiple parent_asin
asin_to_parents = defaultdict(set)
for r in rows:
    asin_to_parents[r["asin"]].add(r["parent_asin"])

asin_multi_parent = sum(1 for parents in asin_to_parents.values() if len(parents) > 1)
print("\n=== TASK 8: asin -> multiple parent_asin ===")
print(f"[observed] Distinct asin values total: {len(asin_to_parents)}")
print(f"[observed] Distinct asin values mapping to >1 parent_asin: {asin_multi_parent}")

print("\nDONE")
