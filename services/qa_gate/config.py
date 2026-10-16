"""
QA gate settings for the vendor_items CDC stream. Every value can be overridden with an
environment variable (see docker-compose.yml, service qa-gate); the defaults are tuned on the
generated data, where legitimate price changes stay between 0.67x and 1.5x of the previous price.
"""
import os
from dataclasses import dataclass
from datetime import datetime, timezone


def _env(name, default, cast=str):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return cast(raw.strip())
    except ValueError as e:
        raise ValueError(f"invalid value for {name}: {raw!r}") from e


def _bool(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _utc(v) -> datetime:
    dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class GateConfig:
    # --- suspicious price changes (compared with the last ACCEPTED price of the same item) ---
    price_max_increase_ratio: float = 1.6      # new / old above this  -> quarantine
    price_min_decrease_ratio: float = 0.6      # new / old below this  -> quarantine
    price_jump_min_abs_egp: float = 0.5        # ignore jumps smaller than this (cheap items)
    quarantine_confirm_count: int = 3          # N consecutive agreeing suspicious prices release the
                                               # quarantine and become the new baseline (0 = never release)
    price_confirm_tolerance: float = 0.02      # "agreeing" = within 2 % of each other
    # --- ordering and duplicates ---
    stale_action: str = "dlq"                  # dlq: older events go to the DLQ
                                               # accept: pass downstream flagged late_arrival, never touch the gate state
    dedup_window: int = 16                     # src_lsn values remembered per item
    # --- timestamps ---
    min_event_time: datetime = datetime(2020, 1, 1, tzinfo=timezone.utc)
    max_future_skew_seconds: int = 0           # 0 = do not check (replayed data can be dated after the wall clock)
    # --- state ---
    stateful: bool = True                      # duplicates / stale / price-jump checks (False = field checks only)
    rebuild_state: bool = True                 # on start, re-read vendor_items.valid to restore the item state

    def __post_init__(self):
        if not self.price_max_increase_ratio > 1:
            raise ValueError("PRICE_MAX_INCREASE_RATIO must be > 1")
        if not 0 < self.price_min_decrease_ratio < 1:
            raise ValueError("PRICE_MIN_DECREASE_RATIO must be between 0 and 1")
        if self.stale_action not in ("dlq", "accept"):
            raise ValueError("STALE_ACTION must be 'dlq' or 'accept'")
        if self.dedup_window < 1 or self.quarantine_confirm_count < 0:
            raise ValueError("DEDUP_WINDOW must be >= 1 and QUARANTINE_CONFIRM_COUNT >= 0")

    @classmethod
    def from_env(cls) -> "GateConfig":
        d = cls()
        return cls(
            price_max_increase_ratio=_env("PRICE_MAX_INCREASE_RATIO", d.price_max_increase_ratio, float),
            price_min_decrease_ratio=_env("PRICE_MIN_DECREASE_RATIO", d.price_min_decrease_ratio, float),
            price_jump_min_abs_egp=_env("PRICE_JUMP_MIN_ABS_EGP", d.price_jump_min_abs_egp, float),
            quarantine_confirm_count=_env("QUARANTINE_CONFIRM_COUNT", d.quarantine_confirm_count, int),
            price_confirm_tolerance=_env("PRICE_CONFIRM_TOLERANCE", d.price_confirm_tolerance, float),
            stale_action=_env("STALE_ACTION", d.stale_action).lower(),
            dedup_window=_env("DEDUP_WINDOW", d.dedup_window, int),
            min_event_time=_env("MIN_EVENT_TIME", d.min_event_time, _utc),
            max_future_skew_seconds=_env("MAX_FUTURE_SKEW_SECONDS", d.max_future_skew_seconds, int),
            stateful=_env("STATEFUL_CHECKS", d.stateful, _bool),
            rebuild_state=_env("STATE_REBUILD", d.rebuild_state, _bool),
        )


DEFAULT_CONFIG = GateConfig()
