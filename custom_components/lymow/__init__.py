"""Lymow integration package."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry

from .const import DOMAIN
from .hub import LymowHub

_LOGGER = logging.getLogger(__name__)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    hass.data.setdefault(DOMAIN, {})
    _LOGGER.debug("Lymow component setup")
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    host = entry.data.get("host")
    hub = LymowHub(host)
    await hub.async_connect()

    hass.data[DOMAIN][entry.entry_id] = hub

    # Forward platforms
    hass.async_create_task(
        hass.config_entries.async_forward_entry_setup(entry, "sensor")
    )
    _LOGGER.info("Lymow entry set up for host %s", host)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})
    hub: LymowHub | None = hass.data[DOMAIN].pop(entry.entry_id, None)
    if hub:
        await hub.async_disconnect()

    await hass.config_entries.async_forward_entry_unload(entry, "sensor")
    return True
