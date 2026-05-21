"""Lymow sensor platform."""
from __future__ import annotations

from typing import Any
import logging

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, CONF_LYMOW_THING_NAME
from .hub import LymowHub

_LOGGER = logging.getLogger(__name__)

SENSOR_TYPES = {
    "battery": {
        "name": "Battery",
        "unit_of_measurement": "%",
        "device_class": "battery",
        "state_class": "measurement",
        "icon": "mdi:battery",
    },
    "robot_status": {
        "name": "Robot Status",
        "icon": "mdi:robot-mower",
    },
    "robot_status_code": {
        "name": "Robot Status Code",
        "icon": "mdi:robot-mower",
    },
    "work_status_code": {
        "name": "Work Status Code",
        "icon": "mdi:state-machine",
    },
    "wifi_signal": {
        "name": "WiFi Signal",
        "unit_of_measurement": "dBm",
        "device_class": "signal_strength",
        "state_class": "measurement",
        "icon": "mdi:wifi",
    },
    "lte_signal": {
        "name": "LTE Signal",
        "unit_of_measurement": "dBm",
        "device_class": "signal_strength",
        "state_class": "measurement",
        "icon": "mdi:signal-4g",
    },
    "last_update": {
        "name": "Last Update",
        "icon": "mdi:clock-outline",
    },
    "map_svg_url": {
    "name": "Map SVG URL",
    "icon": "mdi:image",
    },

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
        [LymowSensor(entry.entry_id, hub, key, device_info) for key in SENSOR_TYPES]
    )


class LymowSensor(SensorEntity):
    """Representation of a Lymow sensor value."""

    def __init__(self, entry_id: str, hub: LymowHub, state_key: str, device_info: DeviceInfo) -> None:
        self._entry_id = entry_id
        self._hub = hub
        self._state_key = state_key
        self._attr_name = SENSOR_TYPES[state_key]["name"]
        self._attr_native_unit_of_measurement = SENSOR_TYPES[state_key].get("unit_of_measurement")
        self._attr_device_class = SENSOR_TYPES[state_key].get("device_class")
        self._attr_state_class = SENSOR_TYPES[state_key].get("state_class")
        self._attr_icon = SENSOR_TYPES[state_key].get("icon")
        self._attr_unique_id = f"{DOMAIN}_{entry_id}_{state_key}"
        self._attr_device_info = device_info

    @property
    def native_value(self) -> Any:
        return self._hub.state.get(self._state_key)

    @property
    def available(self) -> bool:
        return self._hub.available

    async def async_added_to_hass(self) -> None:
        self._hub.async_register_listener(self.async_write_ha_state)

    async def async_will_remove_from_hass(self) -> None:
        self._hub.async_remove_listener(self.async_write_ha_state)
