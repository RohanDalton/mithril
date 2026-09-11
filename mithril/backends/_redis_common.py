"""
Disjoint Redis key families, shared by synchronous and asynchronous backends.

Upgrades from the original shared-lock key format require draining all holders.
"""

from __future__ import annotations

import math
from urllib.parse import quote

from .._types import LockMode, validate_duration
from ..exceptions import ConfigurationError

__author__ = "Rohan B. Dalton"


class RedisKeys:
    _namespace: str

    def _key(self, kind: str, lock_name: str) -> str:
        if not isinstance(lock_name, str) or not lock_name:
            raise ConfigurationError("lock_name must be a nonempty string")
        key = f"{kind}:{quote(lock_name, safe='')}"
        return f"{quote(self._namespace, safe='')}:{key}" if self._namespace else key

    def _lock_key(self, lock_name: str) -> str:
        return self._key("lock", lock_name)

    def _fence_key(self, lock_name: str) -> str:
        return self._key("fence", lock_name)

    def _signal_key(self, lock_name: str) -> str:
        return self._key("signal", lock_name)

    def _meta_key(self, lock_name: str) -> str:
        return self._key("meta", lock_name)

    def _readers_key(self, lock_name: str) -> str:
        return self._key("readers", lock_name)

    def _readers_total_key(self, lock_name: str) -> str:
        return self._key("reader-deadlines", lock_name)


def ttl_milliseconds(ttl_seconds: float) -> int:
    validate_duration(ttl_seconds, "ttl_seconds")
    return max(1, math.ceil(ttl_seconds * 1000))


def validate_mode(mode: LockMode) -> None:
    if not isinstance(mode, LockMode):
        raise ConfigurationError("mode must be a LockMode")
