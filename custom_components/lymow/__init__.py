"""Lymow integration package."""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN, PLATFORMS
from .hub import LymowHub

_LOGGER = logging.getLogger(__name__)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the Lymow integration."""
    hass.data.setdefault(DOMAIN, {})
    _LOGGER.debug("Lymow integration setup")
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Lymow from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    hub = LymowHub(hass, entry.data)

    try:
        await hub.async_connect()
    except Exception:
        _LOGGER.exception("Failed to connect Lymow hub")
        return False

    hass.data[DOMAIN][entry.entry_id] = hub

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    _LOGGER.info("Lymow entry set up for %s", entry.data.get("lymow_thing_name"))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a Lymow config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    hub: LymowHub | None = hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    if hub:
        await hub.async_disconnect()

    return unload_ok
