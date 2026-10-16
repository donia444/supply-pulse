"""
Unit tests for the vendor_items QA gate: field checks, normalisation, duplicates, stale / out-of-order
events, price quarantine, deletes and snapshot events. No Kafka and no database needed.

    pip install pytest pydantic tzdata confluent-kafka
    pytest tests/qa_gate
"""
import os
import sys
from dataclasses import replace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "qa_gate"))

import pytest  # noqa: E40

from services.qa_gate.config import GateConfig  # noqa: E402
from services.qa_gate.gate import DLQ_TOPIC, QUARANTINE_TOPIC, VALID_TOPIC, process  # noqa: E402
from services.qa_gate.rules import Reference  # noqa: E402
from services.qa_gate.state import ItemStateStore  # noqa: E402

REF = Reference(
    integration={1: "API", 2: "POS_LEGACY"},
    list_price={"SKU-00001": 50.0, "M0000001": 100.0},
    products={"SKU-00001", "SKU-00002"},
    menu_vendor={"M0000001": 1},
)
CFG = GateConfig()


def row(minute=0, lsn=1, **kw):
    r = {"store_id": 1, "item_ref": "SKU-00001", "src_lsn": lsn, "item_name": "Milk 1 L", "item_price": "50.0",
         "currency": "EGP", "qty_on_hand": "10", "is_available": "true", "updated_by": "wms_sync",
         "updated_at": f"2026-09-10 10:{minute:02d}:00"}
    r.update(kw)
    return r


def env(op="u", after=None, before=None):
    return {"op": op, "after": after, "before": before, "ts_ms": 1, "source": {"lsn": 99}}


def run(store, op="u", cfg=CFG, **kw):
    return process(env(op, after=row(**kw)), REF, store, cfg)


@pytest.fixture
def store():
    return ItemStateStore(CFG)


# ------------------------------------------------------------------ normalisation and field checks
def test_accepted_event_is_normalised(store):
    topic, key, v = run(store, store_id="1", item_ref=" sku-00001 ", currency="LE", item_name="  Milk   1 L ")
    assert topic == VALID_TOPIC and key == "V0001|SKU-00001"
    assert v["vendor_id"] == "V0001" and v["currency"] == "EGP" and v["item_name"] == "Milk 1 L"
    assert v["event_type"] == "update" and v["event_time_utc"].endswith("Z")


def test_empty_currency_is_assumed_egp(store):
    _, _, v = run(store, currency="")
    assert v["currency"] == "EGP" and v["currency_assumed"] is True


def test_legacy_pos_price_is_converted_from_piasters(store):
    topic, _, v = process(env("c", after=row(store_id=2, item_ref="SKU-00002", item_price="3150", currency="PT")),
                          REF, store, CFG)
    assert topic == VALID_TOPIC and v["price_egp"] == "31.50"


@pytest.mark.parametrize("overrides, reason", [
    ({"store_id": None}, "MISSING_VENDOR_ID"),
    ({"store_id": "abc"}, "INVALID_VENDOR_ID"),
    ({"store_id": 99}, "UNKNOWN_VENDOR"),
    ({"item_ref": "SKU-99999"}, "UNKNOWN_PRODUCT"),
    ({"item_ref": "XYZ"}, "INVALID_ITEM_REF"),
    ({"item_ref": "M0000001", "store_id": 2, "item_price": "5000"}, "ITEM_VENDOR_MISMATCH"),  # POS_LEGACY: 5000 piasters
    ({"item_ref": "M0000009"}, "UNKNOWN_MENU_ITEM"),
    ({"currency": "USD"}, "UNSUPPORTED_CURRENCY"),
    ({"updated_at": None}, "MISSING_TIMESTAMP"),
    ({"updated_at": "not a date"}, "INVALID_TIMESTAMP"),
    ({"updated_at": "1999-01-01 00:00:00"}, "TIMESTAMP_OUT_OF_RANGE"),
    ({"qty_on_hand": "10.5"}, "NON_INTEGER_STOCK"),
    ({"qty_on_hand": "-3", "is_available": "false"}, "NEGATIVE_STOCK"),
    ({"is_available": "maybe"}, "INVALID_AVAILABILITY"),
    ({"item_name": ""}, "NULL_ITEM_NAME"),
    ({"item_price": "0"}, "NON_POSITIVE_PRICE"),
    ({"item_price": "400"}, "PRICE_OUTLIER"),
    ({"src_lsn": "12x"}, "INVALID_LSN"),
])
def test_field_checks_reject_with_reason(store, overrides, reason):
    topic, _, v = run(store, **overrides)
    assert topic == DLQ_TOPIC and v["reject_reason"] == reason
    assert v["raw"]["item_name"] is not None or reason == "NULL_ITEM_NAME"   # the raw event is preserved


def test_rejected_event_never_creates_state(store):
    run(store, minute=5, qty_on_hand="-1", is_available="false")
    assert len(store) == 0


# ------------------------------------------------------------------ duplicates, stale, out of order
def test_duplicate_src_lsn_goes_to_dlq(store):
    assert run(store, minute=1, lsn=10)[0] == VALID_TOPIC
    topic, _, v = run(store, minute=1, lsn=10)
    assert topic == DLQ_TOPIC and v["reject_reason"] == "DUPLICATE_EVENT"


def test_same_values_with_new_lsn_is_accepted_and_flagged(store):
    run(store, minute=1, lsn=10)
    topic, _, v = run(store, minute=2, lsn=11)
    assert topic == VALID_TOPIC and v["no_change"] is True


def test_older_event_is_stale_and_keeps_the_newer_state(store):
    run(store, minute=10, lsn=10, item_price="50.0")
    topic, _, v = run(store, minute=5, lsn=11, item_price="55.0")
    assert topic == DLQ_TOPIC and v["reject_reason"] == "STALE_EVENT"
    assert v["item"]["price_egp"] == "55.00"                       # clean item kept for a possible replay
    assert store.get("V0001|SKU-00001").price == 50                # the newer version is still current
    # still newer than nothing it could overwrite: an event between the two is stale as well
    assert run(store, minute=7, lsn=12)[2]["reject_reason"] == "STALE_EVENT"


def test_same_second_with_lower_lsn_is_out_of_order(store):
    run(store, minute=3, lsn=20)
    topic, _, v = run(store, minute=3, lsn=19, item_price="52.0")
    assert topic == DLQ_TOPIC and v["reject_reason"] == "OUT_OF_ORDER_EVENT"


def test_same_second_with_higher_lsn_wins(store):
    run(store, minute=3, lsn=20)
    assert run(store, minute=3, lsn=21, item_price="52.0")[0] == VALID_TOPIC


def test_invalid_event_does_not_block_a_later_valid_one(store):
    run(store, minute=2, lsn=1)
    assert run(store, minute=9, lsn=2, qty_on_hand="-5", is_available="false")[0] == DLQ_TOPIC
    assert run(store, minute=4, lsn=3, item_price="51.0")[0] == VALID_TOPIC   # state is still minute 2


def test_stale_action_accept_flags_late_arrival_without_touching_state():
    cfg = replace(CFG, stale_action="accept")
    s = ItemStateStore(cfg)
    run(s, cfg=cfg, minute=10, lsn=10)
    topic, _, v = run(s, cfg=cfg, minute=5, lsn=11, item_price="55.0")
    assert topic == VALID_TOPIC and v["late_arrival"] is True and v["late_reason"] == "STALE_EVENT"
    assert s.get("V0001|SKU-00001").price == 50


# ------------------------------------------------------------------ suspicious prices
def test_price_jump_is_quarantined_and_baseline_is_kept(store):
    run(store, minute=1, lsn=1, item_price="50.0")
    topic, _, v = run(store, minute=2, lsn=2, item_price="100.0")      # 2.0x
    assert topic == QUARANTINE_TOPIC and v["quarantine_reason"] == "SUSPICIOUS_PRICE_CHANGE"
    assert v["detail"]["previous_price_egp"] == "50.00" and v["detail"]["ratio"] == 2.0
    assert run(store, minute=3, lsn=3, item_price="52.0")[0] == VALID_TOPIC   # judged against 50, not 100


def test_price_drop_is_quarantined(store):
    run(store, minute=1, lsn=1, item_price="50.0")
    assert run(store, minute=2, lsn=2, item_price="25.0")[0] == QUARANTINE_TOPIC   # 0.5x


def test_normal_price_changes_pass(store):
    run(store, minute=1, lsn=1, item_price="50.0")
    assert run(store, minute=2, lsn=2, item_price="70.0")[0] == VALID_TOPIC        # 1.4x
    assert run(store, minute=3, lsn=3, item_price="50.0")[0] == VALID_TOPIC        # back down, 0.71x


def test_repeated_new_price_releases_the_quarantine(store):
    run(store, minute=1, lsn=1, item_price="50.0")
    assert run(store, minute=2, lsn=2, item_price="100.0")[0] == QUARANTINE_TOPIC
    assert run(store, minute=3, lsn=3, item_price="100.5")[0] == QUARANTINE_TOPIC
    topic, _, v = run(store, minute=4, lsn=4, item_price="100.0")                  # third agreeing event
    assert topic == VALID_TOPIC and v["quarantine_released"] is True


def test_thresholds_are_configurable():
    cfg = replace(CFG, price_max_increase_ratio=1.2)
    s = ItemStateStore(cfg)
    run(s, cfg=cfg, minute=1, lsn=1, item_price="50.0")
    assert run(s, cfg=cfg, minute=2, lsn=2, item_price="65.0")[0] == QUARANTINE_TOPIC


def test_small_absolute_change_is_not_quarantined(store):
    # SKU-00002 has no list price. 0.30 -> 0.60 is 2x, but only 0.30 EGP (below PRICE_JUMP_MIN_ABS_EGP)
    run(store, minute=1, lsn=1, item_ref="SKU-00002", item_price="0.30")
    assert run(store, minute=2, lsn=2, item_ref="SKU-00002", item_price="0.60")[0] == VALID_TOPIC


# ------------------------------------------------------------------ snapshot, insert, delete
def test_snapshot_insert_update_event_types(store):
    assert run(store, op="r", minute=1, lsn=1)[2]["event_type"] == "snapshot"
    assert run(store, op="u", minute=2, lsn=2, item_price="51.0")[2]["event_type"] == "update"
    assert process(env("c", after=row(item_ref="SKU-00002", minute=1, lsn=3)), REF, store, CFG)[2]["event_type"] == "insert"


def test_resnapshot_older_than_state_is_stale(store):
    run(store, op="u", minute=20, lsn=5)
    topic, _, v = run(store, op="r", minute=0, lsn=1)
    assert topic == DLQ_TOPIC and v["reject_reason"] == "STALE_EVENT"


def test_delete_closes_the_item_and_second_delete_is_a_duplicate(store):
    run(store, minute=1, lsn=1)
    topic, key, v = process(env("d", before=row()), REF, store, CFG)
    assert topic == VALID_TOPIC and key == "V0001|SKU-00001" and v["event_type"] == "delete"
    topic, _, v = process(env("d", before=row()), REF, store, CFG)
    assert topic == DLQ_TOPIC and v["reject_reason"] == "DUPLICATE_DELETE"


def test_update_after_delete_relists_without_a_price_jump_check(store):
    run(store, minute=1, lsn=1, item_price="50.0")
    process(env("d", before=row()), REF, store, CFG)
    topic, _, v = run(store, op="c", minute=5, lsn=2, item_price="120.0")           # would be 2.4x, but a new listing
    assert topic == VALID_TOPIC and v["relisted"] is True


def test_older_update_after_delete_stays_stale(store):
    run(store, minute=10, lsn=1)
    process(env("d", before=row()), REF, store, CFG)
    assert run(store, minute=2, lsn=2)[2]["reject_reason"] == "STALE_EVENT"


def test_delete_without_a_readable_key_goes_to_dlq(store):
    topic, _, v = process(env("d", before={"item_ref": "SKU-00001"}), REF, store, CFG)
    assert topic == DLQ_TOPIC and v["reject_reason"] == "MISSING_KEY"


def test_update_without_after_image_goes_to_dlq(store):
    topic, _, v = process(env("u", after=None), REF, store, CFG)
    assert topic == DLQ_TOPIC and v["reject_reason"] == "MISSING_AFTER_IMAGE"


def test_truncate_event_is_ignored(store):
    assert process({"op": "t", "source": {}}, REF, store, CFG) is None


# ------------------------------------------------------------------ restart
def test_state_restored_from_valid_topic_still_detects_duplicates_and_stale_events():
    first = ItemStateStore(CFG)
    _, _, accepted = run(first, minute=10, lsn=10)
    restored = ItemStateStore(CFG)
    restored.restore(accepted)                                                      # what rebuild_state() replays
    assert run(restored, minute=10, lsn=10)[2]["reject_reason"] == "DUPLICATE_EVENT"
    assert run(restored, minute=1, lsn=11)[2]["reject_reason"] == "STALE_EVENT"


def test_stateless_mode_only_runs_field_checks():
    assert process(env("u", after=row(minute=10, lsn=10)), REF, None, CFG)[0] == VALID_TOPIC
    assert process(env("u", after=row(minute=1, lsn=10)), REF, None, CFG)[0] == VALID_TOPIC
