"""
API Quota Manager for tracking and managing API rate limits.
"""
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Lock
from typing import Dict, Optional

from prometheus_client import Gauge

logger = logging.getLogger(__name__)


# Prometheus metrics
QUOTA_USAGE = Gauge(
    'api_quota_usage',
    'Current API quota usage',
    ['platform']
)

QUOTA_REMAINING = Gauge(
    'api_quota_remaining',
    'Remaining API quota',
    ['platform']
)


@dataclass
class QuotaConfig:
    """Configuration for API quota."""
    limit: int
    window_seconds: int
    cost_per_request: int = 1
    reset_time: Optional[datetime] = None
    used: int = 0


@dataclass
class PlatformQuota:
    """Quota tracking for a platform."""
    config: QuotaConfig
    requests: list = field(default_factory=list)
    lock: Lock = field(default_factory=Lock)


class QuotaManager:
    """
    Manages API quota across multiple platforms.

    Features:
    - Per-platform quota tracking
    - Sliding window rate limiting
    - Automatic reset handling
    - Prometheus metrics export
    """

    # Default quota configurations
    DEFAULT_QUOTAS = {
        'youtube': QuotaConfig(
            limit=10000,
            window_seconds=86400,  # Daily
            cost_per_request=1,
        ),
        'twitter': QuotaConfig(
            limit=450,
            window_seconds=900,  # 15 minutes
            cost_per_request=1,
        ),
        'naver_shopping': QuotaConfig(
            limit=25000,
            window_seconds=86400,  # Daily
            cost_per_request=1,
        ),
        'tiktok': QuotaConfig(
            limit=100,
            window_seconds=60,  # Per minute
            cost_per_request=1,
        ),
        'google_trends': QuotaConfig(
            limit=100,
            window_seconds=60,  # Per minute
            cost_per_request=1,
        ),
    }

    def __init__(self, custom_quotas: Optional[Dict[str, QuotaConfig]] = None):
        """
        Initialize quota manager.

        Args:
            custom_quotas: Custom quota configurations
        """
        self._quotas: Dict[str, PlatformQuota] = {}

        # Initialize with default quotas
        for platform, config in self.DEFAULT_QUOTAS.items():
            self._quotas[platform] = PlatformQuota(config=config)

        # Override with custom quotas
        if custom_quotas:
            for platform, config in custom_quotas.items():
                self._quotas[platform] = PlatformQuota(config=config)

        logger.info(f"Initialized quota manager for {len(self._quotas)} platforms")

    def _cleanup_old_requests(self, platform: str) -> None:
        """Remove requests outside the current window."""
        quota = self._quotas.get(platform)
        if not quota:
            return

        cutoff = datetime.utcnow() - timedelta(seconds=quota.config.window_seconds)
        quota.requests = [r for r in quota.requests if r > cutoff]

    def can_make_request(self, platform: str, cost: int = 1) -> bool:
        """
        Check if a request can be made within quota limits.

        Args:
            platform: Platform name
            cost: Cost of the request

        Returns:
            True if request is allowed
        """
        quota = self._quotas.get(platform)
        if not quota:
            logger.warning(f"No quota config for platform: {platform}")
            return True

        with quota.lock:
            self._cleanup_old_requests(platform)

            current_usage = sum(1 for _ in quota.requests)
            remaining = quota.config.limit - current_usage

            return remaining >= cost

    def record_request(self, platform: str, cost: int = 1) -> bool:
        """
        Record a request and update quota.

        Args:
            platform: Platform name
            cost: Cost of the request

        Returns:
            True if request was recorded successfully
        """
        quota = self._quotas.get(platform)
        if not quota:
            logger.warning(f"No quota config for platform: {platform}")
            return True

        with quota.lock:
            self._cleanup_old_requests(platform)

            current_usage = sum(1 for _ in quota.requests)
            remaining = quota.config.limit - current_usage

            if remaining < cost:
                logger.warning(f"Quota exceeded for {platform}")
                return False

            # Record the request
            now = datetime.utcnow()
            for _ in range(cost):
                quota.requests.append(now)

            # Update metrics
            new_usage = current_usage + cost
            QUOTA_USAGE.labels(platform=platform).set(new_usage)
            QUOTA_REMAINING.labels(platform=platform).set(quota.config.limit - new_usage)

            return True

    def get_remaining(self, platform: str) -> int:
        """
        Get remaining quota for a platform.

        Args:
            platform: Platform name

        Returns:
            Remaining quota units
        """
        quota = self._quotas.get(platform)
        if not quota:
            return -1

        with quota.lock:
            self._cleanup_old_requests(platform)
            current_usage = sum(1 for _ in quota.requests)
            return quota.config.limit - current_usage

    def get_wait_time(self, platform: str) -> float:
        """
        Get time to wait before next request is allowed.

        Args:
            platform: Platform name

        Returns:
            Seconds to wait, 0 if request is allowed
        """
        quota = self._quotas.get(platform)
        if not quota:
            return 0

        with quota.lock:
            self._cleanup_old_requests(platform)

            if len(quota.requests) < quota.config.limit:
                return 0

            # Find oldest request
            oldest = min(quota.requests)
            reset_time = oldest + timedelta(seconds=quota.config.window_seconds)
            wait_time = (reset_time - datetime.utcnow()).total_seconds()

            return max(0, wait_time)

    def wait_for_quota(self, platform: str, cost: int = 1) -> None:
        """
        Wait until quota is available.

        Args:
            platform: Platform name
            cost: Cost of the request
        """
        while not self.can_make_request(platform, cost):
            wait_time = self.get_wait_time(platform)
            if wait_time > 0:
                logger.info(f"Waiting {wait_time:.1f}s for {platform} quota")
                time.sleep(min(wait_time, 60))  # Max 60s wait

    def get_all_status(self) -> Dict[str, Dict]:
        """
        Get quota status for all platforms.

        Returns:
            Dict of platform quota status
        """
        status = {}
        for platform, quota in self._quotas.items():
            with quota.lock:
                self._cleanup_old_requests(platform)
                usage = sum(1 for _ in quota.requests)
                status[platform] = {
                    'limit': quota.config.limit,
                    'used': usage,
                    'remaining': quota.config.limit - usage,
                    'window_seconds': quota.config.window_seconds,
                }
        return status

    def reset(self, platform: Optional[str] = None) -> None:
        """
        Reset quota counters.

        Args:
            platform: Platform to reset, or None for all
        """
        if platform:
            quota = self._quotas.get(platform)
            if quota:
                with quota.lock:
                    quota.requests.clear()
                logger.info(f"Reset quota for {platform}")
        else:
            for p, quota in self._quotas.items():
                with quota.lock:
                    quota.requests.clear()
            logger.info("Reset all platform quotas")


# Global instance
_quota_manager: Optional[QuotaManager] = None


def get_quota_manager() -> QuotaManager:
    """Get or create the global quota manager instance."""
    global _quota_manager
    if _quota_manager is None:
        _quota_manager = QuotaManager()
    return _quota_manager
