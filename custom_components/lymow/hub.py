"""Simple hub/client for Lymow device interactions."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

_LOGGER = logging.getLogger(__name__)


class LymowHub:
    """Minimal hub used by the integration as a placeholder for real client code."""

    def __init__(self, host: str | None = None) -> None:
        self.host = host
        self._connected = False

    async def async_connect(self) -> None:
        _LOGGER.debug("Connecting to Lymow host %s", self.host)
        # Placeholder for real connection logic
        await asyncio.sleep(0)
        self._connected = True

    async def async_disconnect(self) -> None:
        _LOGGER.debug("Disconnecting from Lymow host %s", self.host)
        await asyncio.sleep(0)
        self._connected = False

    async def async_get_state(self) -> dict[str, Any]:
        """Return a sample state dict for sensors to consume.

        Replace with real API calls to the Lymow device.
        """
        # Placeholder values
        return {
            "uptime": 12345,
            "status": "ok",
        }
