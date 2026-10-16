from datetime import datetime, timezone

from enhanced_gate import VALID_TOPIC, EnhancedGate
from enhanced_rules import EnhancedValidator
from redis_state_manager import RedisStateManager
from rules import Reference
from tests.conftest import FakeRedis


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _cdc(op, after=None, before=None):
    return {"op": op, "ts_ms": 1, "source": {"lsn": 99}, "after": after, "before": before}


def test_snapshot_insert_update_delete(tmp_path):
    ref = Reference(integration={1: "API"}, list_price={"SKU-00001": 10.0})
    validator = EnhancedValidator(reference=ref, known_skus={"SKU-00001"})
    state = RedisStateManager(client=FakeRedis())
    g = EnhancedGate(state=state, validator=validator, output_dir=str(tmp_path))
    after = {
        "store_id": 1,
        "item_ref": "SKU-00001",
        "item_name": "Milk",
        "item_price": "10.50",
        "currency": "EGP",
        "qty_on_hand": "8",
        "is_available": "true",
        "updated_at": "2026-10-13T10:00:00Z",
        "src_lsn": 1,
    }
    topic, _, body = g.route_event(_cdc("r", after=after))
    assert topic == VALID_TOPIC
    assert state.get_current("V0001", "SKU-00001")["price"] == 10.5

    after_u = dict(after, item_price="11.00", src_lsn=2, updated_at="2026-10-13T11:00:00Z")
    g.route_event(_cdc("u", after=after_u))
    assert state.get_current("V0001", "SKU-00001")["price"] == 11.0

    topic, _, _ = g.route_event(_cdc("d", before=after_u))
    assert topic == VALID_TOPIC
    assert state.get_current("V0001", "SKU-00001") is None


def test_last_known_good_preserved(tmp_path):
    validator = EnhancedValidator()
    state = RedisStateManager(client=FakeRedis())
    g = EnhancedGate(state=state, validator=validator, output_dir=str(tmp_path))
    g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": 20.0, "currency": "EGP",
        "stock": 3, "availability": True, "observation_timestamp": _now(),
    })
    _, _, body = g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": "not-a-number", "currency": "EGP",
        "stock": 3, "availability": True, "observation_timestamp": _now(),
    })
    assert body["status"] == "rejected"
    assert body["last_known_good"]["price"] == 20.0
    assert state.get_current("V0001", "SKU-001")["price"] == 20.0


def test_historical_not_only_redis(tmp_path):
    g = EnhancedGate(state=RedisStateManager(client=FakeRedis()), validator=EnhancedValidator(), output_dir=str(tmp_path))
    g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": 8.0, "currency": "EGP",
        "stock": 2, "availability": True, "observation_timestamp": _now(),
    })
    g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": -1, "currency": "EGP",
        "stock": 2, "availability": True, "observation_timestamp": _now(),
    })
    assert (tmp_path / "accepted.jsonl").read_text(encoding="utf-8").strip()
    assert (tmp_path / "rejected.jsonl").read_text(encoding="utf-8").strip()


def test_carrefour_sample_csv_is_labeled_non_live():
    from carrefour_collector import bundled_sample_path, collect_from_csv

    rows = collect_from_csv(bundled_sample_path())
    assert rows
    assert all(r["is_live"] is False for r in rows)
    assert all(r["data_label"] == "SAMPLE_DATA_NON_LIVE" for r in rows)
    assert all(r["vendor_id"] == "CARREFOUR_EG" for r in rows)
