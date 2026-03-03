from mithril import exceptions as exc

__author__ = "Rohan B. Dalton"


def test_exception_hierarchy() -> None:
    """
    GIVEN: Any Mithril exception classes
    WHEN:  The inheritance is inspected
    THEN:  Each subclass is correctly related to its base
    """
    assert issubclass(exc.LockAcquisitionError, exc.MithrilError)
    assert issubclass(exc.LockTimeoutError, exc.LockAcquisitionError)
    assert issubclass(exc.AlreadyAcquiredError, exc.LockAcquisitionError)
    assert issubclass(exc.LockReleaseError, exc.MithrilError)
    assert issubclass(exc.NotAcquiredError, exc.LockReleaseError)
    assert issubclass(exc.LockExpiredError, exc.LockReleaseError)
    assert issubclass(exc.LockRenewalError, exc.MithrilError)
    assert issubclass(exc.RenewalFailedError, exc.LockRenewalError)
    assert issubclass(exc.AutoRenewalStoppedError, exc.LockRenewalError)
    assert issubclass(exc.FencingTokenError, exc.MithrilError)
    assert issubclass(exc.StaleTokenError, exc.FencingTokenError)
    assert issubclass(exc.BackendError, exc.MithrilError)
    assert issubclass(exc.BackendUnavailableError, exc.BackendError)
    assert issubclass(exc.BackendTimeoutError, exc.BackendError)
    assert issubclass(exc.ConfigurationError, exc.MithrilError)


if __name__ == "__main__":
    pass
