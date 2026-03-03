import dataclasses

import pytest

from mithril._types import LockConfig, LockToken
from mithril.exceptions import ConfigurationError

__author__ = "Rohan B. Dalton"


def test_lock_config_validates_ttl_non_positive() -> None:
    """
    GIVEN: A lock config with non-positive ttl
    WHEN:  It is constructed
    THEN:  ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        LockConfig(name="lock", ttl=0)


def test_lock_config_validates_timeout_negative() -> None:
    """
    GIVEN: A lock config with negative timeout
    WHEN:  It is constructed
    THEN:  ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        LockConfig(name="lock", timeout=-1)


def test_lock_config_validates_retry_interval_positive() -> None:
    """
    GIVEN: A lock config with non-positive retry interval
    WHEN:  It is constructed
    THEN:  ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        LockConfig(name="lock", retry_interval=0)
    with pytest.raises(ConfigurationError):
        LockConfig(name="lock", retry_interval=-1)


def test_lock_config_requires_ttl_for_auto_renewal() -> None:
    """
    GIVEN: Auto-renewal enabled with ttl unset
    WHEN:  It is constructed
    THEN:  ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        LockConfig(name="lock", ttl=None, auto_renewal=True)


def test_lock_token_is_immutable() -> None:
    """
    GIVEN: A lock token instance
    WHEN:  A field mutation is attempted
    THEN:  FrozenInstanceError is raised
    """
    token = LockToken(
        lock_name="lock",
        owner_id="owner",
        fencing_token=1,
        epoch=1,
        acquired_at=1.0,
        expire_at=2.0,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        token.fencing_token = 2  # type: ignore[misc]


if __name__ == "__main__":
    pass
