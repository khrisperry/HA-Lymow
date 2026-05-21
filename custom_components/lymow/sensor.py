"""Lymow sensor platform."""
from __future__ import annotations

from typing import Any
import logging

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    hub = hass.data[DOMAIN].get(entry.entry_id)
    if not hub:
        _LOGGER.error("No hub instance found for entry %s", entry.entry_id)
        return

    state = await hub.async_get_state()
    async_add_entities([LymowSensor(entry.entry_id, hub, state)])


class LymowSensor(SensorEntity):
    """Representation of a Lymow sensor value."""

    def __init__(self, entry_id: str, hub: Any, initial_state: dict[str, Any]) -> None:
        self._entry_id = entry_id
        self._hub = hub
        self._attr_name = "Lymow Uptime"
        self._state = initial_state.get("uptime")

    @property
    def native_value(self):
        return self._state

    @property
    def unique_id(self) -> str:
        return f"lymow_{self._entry_id}_uptime"

    async def async_update(self) -> None:
        state = await self._hub.async_get_state()
        self._state = state.get("uptime")
