"""Custom exception hierarchy for the Watchdog engine.

Callers can distinguish:
  - TransientWatchdogError: retryable (network blip, metastore unavailable)
  - PermanentWatchdogError: not retryable (bad config, missing grants)

Bare Exception catches in crawler / policy-loader paths should wrap the
underlying error into one of these so downstream retry + error-reporting
logic can make routing decisions instead of string-matching messages.
"""


class WatchdogError(Exception):
    """Base class for all Watchdog-raised errors."""


class TransientWatchdogError(WatchdogError):
    """Raised for failures that are expected to resolve on retry.

    Examples: temporary metastore unavailability, rate-limited SDK calls,
    network timeouts.
    """


class PermanentWatchdogError(WatchdogError):
    """Raised for failures that retrying will not fix.

    Examples: malformed policy YAML, missing required config,
    insufficient grants on a target object.
    """


class CrawlError(WatchdogError):
    """Base class for resource-crawl failures.

    Carries the resource_type so partial failures can be routed per-type
    instead of aggregating into an opaque error list.
    """

    def __init__(self, resource_type: str, message: str) -> None:
        super().__init__(f"{resource_type}: {message}")
        self.resource_type = resource_type
        self.message = message


class TransientCrawlError(CrawlError, TransientWatchdogError):
    """A crawl failure that should be retried on the next scan."""


class PermanentCrawlError(CrawlError, PermanentWatchdogError):
    """A crawl failure that will keep failing until config/grants change."""


class PolicyLoadError(PermanentWatchdogError):
    """Raised when a policy document cannot be parsed or validated."""


class RemediationError(WatchdogError):
    """Base class for remediation execution failures."""


class RemediationVerificationError(RemediationError):
    """Post-apply verification detected the remediation did not take effect."""
