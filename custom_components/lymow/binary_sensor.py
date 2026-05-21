"""Lymow binary sensor platform."""
from __future__ import annotations

import logging

from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, CONF_LYMOW_THING_NAME
from .hub import LymowHub

_LOGGER = logging.getLogger(__name__)

BINARY_SENSOR_TYPES = {
    "connected": {
        "name": "Connected",
        "device_class": "connectivity",
        "icon": "mdi:lan-connect",
    }
}


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    hub = hass.data[DOMAIN].get(entry.entry_id)
    if not hub:
        _LOGGER.error("No hub instance found for entry %s", entry.entry_id)
        return

    device_info = DeviceInfo(
        identifiers={(DOMAIN, entry.entry_id)},
        name=entry.data.get(CONF_LYMOW_THING_NAME, "Lymow"),
        manufacturer="Lymow",
        model="Lymow One Plus",
    )

    async_add_entities(
        [LymowBinarySensor(entry.entry_id, hub, key, device_info) for key in BINARY_SENSOR_TYPES]
    )


class LymowBinarySensor(BinarySensorEntity):
    """Representation of a Lymow binary sensor."""

    def __init__(self, entry_id: str, hub: LymowHub, state_key: str, device_info: DeviceInfo) -> None:
        self._entry_id = entry_id
        self._hub = hub
        self._state_key = state_key
        self._attr_name = BINARY_SENSOR_TYPES[state_key]["name"]
        self._attr_device_class = BINARY_SENSOR_TYPES[state_key].get("device_class")
        self._attr_icon = BINARY_SENSOR_TYPES[state_key].get("icon")
        self._attr_unique_id = f"{DOMAIN}_{entry_id}_{state_key}"
        self._attr_device_info = device_info

    @property
    def is_on(self) -> bool | None:
        value = self._hub.state.get("connection_flag")
        return value == 1

    @property
    def available(self) -> bool:
        return self._hub.available

    async def async_added_to_hass(self) -> None:
        self._hub.async_register_listener(self.async_write_ha_state)

    async def async_will_remove_from_hass(self) -> None:
        self._hub.async_remove_listener(self.async_write_ha_state)
