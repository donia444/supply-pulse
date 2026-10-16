import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for extra in (
    ROOT,
    ROOT / "services" / "qa_gate",
    ROOT / "services" / "redis_state",
    ROOT / "services" / "carrefour_collector",
    ROOT / "services" / "dashboard",
):
    sys.path.insert(0, str(extra))


class FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.kv = {}
        self.lists = {}

    def ping(self):
        return True

    def hgetall(self, k):
        return dict(self.hashes.get(k, {}))

    def hset(self, k, mapping=None, **kwargs):
        self.hashes.setdefault(k, {}).update(mapping or {})
        return len(mapping or {})

    def hincrby(self, k, field, amount=1):
        h = self.hashes.setdefault(k, {})
        h[field] = str(int(float(h.get(field, 0))) + amount)
        return int(h[field])

    def delete(self, k):
        self.hashes.pop(k, None)
        self.kv.pop(k, None)
        self.lists.pop(k, None)
        return 1

    def set(self, k, v, nx=False, ex=None):
        if nx and k in self.kv:
            return False
        self.kv[k] = v
        return True

    def get(self, k):
        return self.kv.get(k)

    def lpush(self, k, v):
        self.lists.setdefault(k, []).insert(0, v)
        return len(self.lists[k])

    def ltrim(self, k, a, b):
        lst = self.lists.get(k, [])
        end = None if b == -1 else b + 1
        self.lists[k] = lst[a:end]

    def lrange(self, k, a, b):
        lst = self.lists.get(k, [])
        end = None if b == -1 else b + 1
        return lst[a:end]

    def scan_iter(self, match=None, count=100):
        import fnmatch
        for k in list(self.hashes):
            if not match or fnmatch.fnmatch(k, match):
                yield k

    def keys(self, pattern):
        return list(self.scan_iter(pattern))


@pytest.fixture
def fake_redis():
    return FakeRedis()
