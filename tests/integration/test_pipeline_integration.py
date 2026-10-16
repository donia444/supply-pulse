import json
from datetime import datetime, timezone

import pytest

from enhanced_gate import DLQ_TOPIC, QUARANTINE_TOPIC, VALID_TOPIC, EnhancedGate, RedisWriteError
from enhanced_rules import EnhancedValidator
from redis_state_manager import RedisStateManager
from tests.conftest import FakeRedis


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _gate(tmp_path, redis=None, validator=None):
    client = redis or FakeRedis()
    state = RedisStateManager(client=client)
    return EnhancedGate(
        state=state,
        validator=validator or EnhancedValidator(),
        output_dir=str(tmp_path),
    ), state


def test_accepted_updates_redis(tmp_path):
    g, state = _gate(tmp_path)
    topic, key, body = g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": 10.5, "currency": "EGP",
        "stock": 4, "availability": True, "observation_timestamp": _now(),
    })
    assert topic == VALID_TOPIC
    assert body["status"] == "accepted"
    stored = state.get_current("V0001", "SKU-001")
    assert stored["price"] == 10.5
    assert (tmp_path / "accepted.jsonl").is_file()


def test_rejected_goes_to_dlq_with_reasons(tmp_path):
    g, state = _gate(tmp_path)
    g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": 10.5, "currency": "EGP",
        "stock": 4, "availability": True, "observation_timestamp": _now(),
    })
    topic, key, body = g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": -3, "currency": "EGP",
        "stock": 4, "availability": True, "observation_timestamp": _now(),
    })
    assert topic == DLQ_TOPIC
    assert "NON_POSITIVE_PRICE" in body["reasons"]
    assert body["raw"]["price"] == -3
    assert state.get_current("V0001", "SKU-001")["price"] == 10.5
    assert body.get("last_known_good", {}).get("price") == 10.5


def test_unsupported_currency_dlq(tmp_path):
    g, _ = _gate(tmp_path)
    topic, _, body = g.route_event({
        "vendor_id": "CARREFOUR_EG", "sku": "SKU-001", "price": 10, "currency": "USD",
        "stock": 1, "availability": True, "observation_timestamp": _now(),
        "source": "carrefour_egypt_csv_fallback", "is_live": False,
    })
    assert topic == DLQ_TOPIC
    assert "UNSUPPORTED_CURRENCY" in body["reasons"]


def test_unknown_sku_dlq(tmp_path):
    g, _ = _gate(tmp_path, validator=EnhancedValidator(known_skus={"SKU-001"}))
    topic, _, body = g.route_event({
        "vendor_id": "V0001", "sku": "NOPE", "price": 10, "currency": "EGP",
        "stock": 1, "availability": True, "observation_timestamp": _now(),
    })
    assert topic == DLQ_TOPIC
    assert "UNKNOWN_SKU" in body["reasons"]


def test_quarantine_does_not_update_redis(tmp_path):
    g, state = _gate(tmp_path)
    g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": 10.0, "currency": "EGP",
        "stock": 4, "availability": True, "observation_timestamp": "2026-10-13T10:00:00Z",
    })
    topic, _, body = g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": 13.0, "currency": "EGP",
        "stock": 4, "availability": True, "observation_timestamp": "2026-10-13T11:00:00Z",
    })
    assert topic == QUARANTINE_TOPIC
    assert body["status"] == "quarantined"
    assert state.get_current("V0001", "SKU-001")["price"] == 10.0
    assert (tmp_path / "quarantined.jsonl").is_file()


def test_redis_failure_does_not_accept(tmp_path):
    class Boom(FakeRedis):
        def hset(self, *args, **kwargs):
            raise ConnectionError("down")

    g, _ = _gate(tmp_path, redis=Boom())
    with pytest.raises(RedisWriteError):
        g.route_event({
            "vendor_id": "V0001", "sku": "SKU-001", "price": 10.5, "currency": "EGP",
            "stock": 4, "availability": True, "observation_timestamp": _now(),
        })
    assert g.stats["accepted"] == 0


def test_older_event_does_not_overwrite_redis(tmp_path):
    g, state = _gate(tmp_path)
    g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": 11.0, "currency": "EGP",
        "stock": 4, "availability": True, "observation_timestamp": "2026-10-13T12:00:00Z",
        "source": "ops.public.vendor_items",
    })
    g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": 9.0, "currency": "EGP",
        "stock": 4, "availability": True, "observation_timestamp": "2026-10-13T08:00:00Z",
        "source": "ops.public.vendor_items",
    })
    assert state.get_current("V0001", "SKU-001")["price"] == 11.0


def test_duplicate_event_id_quarantined(tmp_path):
    v = EnhancedValidator(seen_versions={"V0001:SKU-001": {"dup-1"}})
    g, _ = _gate(tmp_path, validator=v)
    topic, _, body = g.route_event({
        "vendor_id": "V0001", "sku": "SKU-001", "price": 10.5, "currency": "EGP",
        "stock": 4, "availability": True, "observation_timestamp": _now(),
        "event_id": "dup-1",
    })
    assert topic == QUARANTINE_TOPIC
    assert "DUPLICATE_UPDATE" in body["reasons"]
