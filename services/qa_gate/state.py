"""
Per-item state for the vendor_items QA gate (no Kafka in this file).

The field checks in rules.py look at ONE event. These checks need to remember the item:
  * duplicates          the same src_lsn again (Kafka redelivery after a restart)
  * stale / out of order  the event is older than the last ACCEPTED version of the item
  * suspicious prices   a jump against the last accepted price -> quarantine
  * deletes             a delete closes the item; a later insert / update re-lists it

Only ACCEPTED events change the state (commit). A rejected, quarantined, duplicate or older event can
never overwrite a newer valid version. Items are ordered by (event_time_utc, src_lsn): business time
first, the source lsn breaks ties inside the same second.

The state lives in memory. On start the gate rebuilds it from vendor_items.valid (see gate.py), so a
restart does not forget what was accepted.
"""
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional

from config import DEFAULT_CONFIG, GateConfig

ACCEPT, REJECT, QUARANTINE = "accept", "reject", "quarantine"


@dataclass
class Decision:
    action: str                                   # accept | reject | quarantine
    reason: Optional[str] = None                  # reject / quarantine reason
    detail: dict = field(default_factory=dict)    # evidence written next to the reason
    flags: dict = field(default_factory=dict)     # extra fields on an accepted event
    update_state: bool = True                     # False: accepted but must not become the current state


@dataclass
class ItemState:
    last_time: Optional[datetime] = None          # event time of the last accepted version
    last_lsn: Optional[int] = None
    price: Optional[Decimal] = None               # baseline for the price-jump check
    qty: Optional[int] = None
    available: Optional[bool] = None
    deleted: bool = False
    fingerprint: Optional[tuple] = None
    lsns: deque = field(default_factory=deque)    # recent src_lsn values (duplicate window)
    pending_price: Optional[float] = None         # quarantined price waiting for confirmation
    pending_count: int = 0


def _fingerprint(time, price, qty, available, name):
    return (time, price, qty, available, name)


class ItemStateStore:
    def __init__(self, cfg: GateConfig = DEFAULT_CONFIG):
        self.cfg = cfg
        self._items: dict = {}

    def __len__(self):
        return len(self._items)

    def get(self, key: str) -> Optional[ItemState]:
        return self._items.get(key)

    # ------------------------------------------------------------------ insert / update / snapshot
    def evaluate(self, key: str, item) -> Decision:
        """item: rules.CleanItem that already passed the field checks. Does not change the item state
        (only the quarantine confirmation counter)."""
        st = self._items.get(key)
        if st is None:
            return Decision(ACCEPT, flags={"first_seen": True})

        # 1) duplicate: the same source change delivered twice
        fp = _fingerprint(item.event_time_utc, item.price_egp, item.qty_on_hand, item.is_available, item.item_name)
        if item.src_lsn is not None:
            if item.src_lsn in st.lsns:
                return Decision(REJECT, "DUPLICATE_EVENT", {"src_lsn": item.src_lsn})
        elif st.fingerprint is not None and fp == st.fingerprint:
            return Decision(REJECT, "DUPLICATE_EVENT", {"matches": "last accepted version"})

        # 2) stale / out of order: older than what we already accepted for this item
        if st.last_time is not None and item.event_time_utc is not None:
            reason = None
            if item.event_time_utc < st.last_time:
                reason = "STALE_EVENT"
            elif (item.event_time_utc == st.last_time and item.src_lsn is not None
                  and st.last_lsn is not None and item.src_lsn < st.last_lsn):
                reason = "OUT_OF_ORDER_EVENT"           # same instant, but the source log says it is older
            if reason:
                detail = {"event_time_utc": item.event_time_utc.isoformat(),
                          "current_event_time_utc": st.last_time.isoformat(),
                          "src_lsn": item.src_lsn, "current_src_lsn": st.last_lsn}
                if self.cfg.stale_action == "accept":
                    return Decision(ACCEPT, flags={"late_arrival": True, "late_reason": reason}, update_state=False)
                return Decision(REJECT, reason, detail)

        flags = {}
        # 3) suspicious price change against the last accepted price (a re-listed item has no baseline)
        if st.price is not None and not st.deleted and st.price > 0:
            ratio = float(item.price_egp / st.price)
            jump = abs(float(item.price_egp - st.price)) >= self.cfg.price_jump_min_abs_egp
            if jump and (ratio > self.cfg.price_max_increase_ratio or ratio < self.cfg.price_min_decrease_ratio):
                new = float(item.price_egp)
                if st.pending_price and abs(new - st.pending_price) <= self.cfg.price_confirm_tolerance * st.pending_price:
                    st.pending_count += 1
                else:
                    st.pending_count = 1
                st.pending_price = new
                n = self.cfg.quarantine_confirm_count
                if n and st.pending_count >= n:        # the new level was repeated: it is a real repricing
                    return Decision(ACCEPT, flags={"quarantine_released": True, "price_change_ratio": round(ratio, 4)})
                return Decision(QUARANTINE, "SUSPICIOUS_PRICE_CHANGE", {
                    "previous_price_egp": str(st.price), "new_price_egp": str(item.price_egp),
                    "ratio": round(ratio, 4), "confirmations": st.pending_count, "confirmations_needed": n,
                    "max_increase_ratio": self.cfg.price_max_increase_ratio,
                    "min_decrease_ratio": self.cfg.price_min_decrease_ratio})
            if ratio != 1.0:
                flags["price_change_ratio"] = round(ratio, 4)

        if (not st.deleted and st.price == item.price_egp and st.qty == item.qty_on_hand
                and st.available == item.is_available):
            flags["no_change"] = True                  # same values again: accepted, SCD2 can collapse it
        if st.deleted:
            flags["relisted"] = True
        return Decision(ACCEPT, flags=flags)

    def commit(self, key: str, item, decision: Decision):
        """Call after an ACCEPT decision. Late arrivals flagged with update_state=False are skipped."""
        if decision.action != ACCEPT or not decision.update_state:
            return
        self._apply(key, item.event_time_utc, item.src_lsn, item.price_egp, item.qty_on_hand,
                    item.is_available, item.item_name)

    def _apply(self, key, time, lsn, price, qty, available, name):
        st = self._items.get(key)
        if st is None:
            st = self._items[key] = ItemState(lsns=deque(maxlen=self.cfg.dedup_window))
        st.last_time, st.last_lsn = time, lsn
        st.price, st.qty, st.available = price, qty, available
        st.fingerprint = _fingerprint(time, price, qty, available, name)
        st.deleted = False
        st.pending_price, st.pending_count = None, 0
        if lsn is not None:
            st.lsns.append(lsn)

    # ------------------------------------------------------------------ delete
    def evaluate_delete(self, key: str) -> Decision:
        st = self._items.get(key)
        if st is not None and st.deleted:
            return Decision(REJECT, "DUPLICATE_DELETE", {"detail": "item is already deleted"})
        return Decision(ACCEPT, flags={"unknown_key": True} if st is None else {})

    def commit_delete(self, key: str):
        st = self._items.get(key)
        if st is None:
            st = self._items[key] = ItemState(lsns=deque(maxlen=self.cfg.dedup_window))
        st.deleted = True                              # last_time / last_lsn are kept: older updates stay stale
        st.price = st.qty = st.available = st.fingerprint = None
        st.pending_price, st.pending_count = None, 0

    # ------------------------------------------------------------------ restore after a restart
    def restore(self, rec: dict):
        """rec: a message of vendor_items.valid (written by the gate). Replayed in topic order."""
        if rec.get("late_arrival"):
            return                                     # accepted late, never became the current state
        vid, ref = rec.get("vendor_id"), rec.get("item_ref")
        if not vid or not ref:
            return
        key = f"{vid}|{ref}"
        if rec.get("event_type") == "delete" or rec.get("op") == "d":
            self.commit_delete(key)
            return
        try:
            t = rec.get("event_time_utc")
            t = datetime.fromisoformat(str(t).replace("Z", "+00:00")) if t else None
            price = Decimal(str(rec["price_egp"]))
        except (KeyError, ValueError, ArithmeticError):
            return
        lsn = rec.get("src_lsn")
        self._apply(key, t, int(lsn) if lsn is not None else None, price, rec.get("qty_on_hand"),
                    rec.get("is_available"), rec.get("item_name"))
