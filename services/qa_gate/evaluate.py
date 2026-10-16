"""
Offline check of the gate rules against the answer key (no Kafka needed):

    docker compose run --rm --entrypoint python qa-gate evaluate.py

Compares our PASS/REJECT (and reason) with _answer_key/scenario_events.csv,
and our parsed UTC time with event_time_utc.
"""
import collections
import csv
import os
import sys

from reference import load_reference
from rules import Reject, parse_time_utc, validate

DATA = os.environ.get("DATA_DIR", "/data")
base = DATA if os.path.isdir(os.path.join(DATA, "ops_db_cdc")) else os.path.join(DATA, "qcommerce_data")
ref = load_reference(DATA)

expected = {}
with open(os.path.join(base, "_answer_key", "scenario_events.csv"), newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        expected[r["lsn"]] = r

seen, conf, wrong_reason, bad_time, total_time = set(), collections.Counter(), collections.Counter(), 0, 0
examples = []
with open(os.path.join(base, "ops_db_cdc", "vendor_items_events.csv"), newline="", encoding="utf-8") as f:
    for raw in csv.DictReader(f):
        if raw["lsn"] in seen or raw["lsn"] not in expected:
            continue
        seen.add(raw["lsn"])
        exp = expected[raw["lsn"]]
        if raw["op"] == "d":
            got, reason = "PASS", ""
        else:
            try:
                item = validate(raw, ref)
                got, reason = "PASS", ""
                if exp["event_time_utc"]:
                    total_time += 1
                    exp_t = exp["event_time_utc"].replace("Z", "+00:00")
                    if item.event_time_utc is None or abs((item.event_time_utc - __import__("datetime").datetime.fromisoformat(exp_t)).total_seconds()) > 60:
                        bad_time += 1
            except Reject as e:
                got, reason = "REJECT", e.reason
        conf[(exp["expected_gate_result"], got)] += 1
        if exp["expected_gate_result"] == "REJECT" and got == "REJECT" and reason != exp["reject_reason"]:
            wrong_reason[(exp["reject_reason"], reason)] += 1
        if exp["expected_gate_result"] != got and len(examples) < 5:
            examples.append((raw["lsn"], exp["scenario"], exp["expected_gate_result"], exp["reject_reason"], got, reason, raw["item_price"], raw["qty_on_hand"], raw["is_available"]))

print("events checked:", len(seen))
print("expected -> got:", dict(conf))
print("same REJECT but different reason:", dict(wrong_reason))
print(f"event time mismatches (>60 s; the AM/PM format has no seconds): {bad_time} of {total_time} passed events")
for e in examples:
    print("mismatch example:", e)
wrong = conf[("PASS", "REJECT")] + conf[("REJECT", "PASS")]
print("RESULT:", "OK" if wrong <= 5 else f"FAILED ({wrong} disagreements)")
sys.exit(0 if wrong <= 5 else 1)
