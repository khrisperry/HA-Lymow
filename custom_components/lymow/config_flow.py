"""Config flow for Lymow integration."""
from __future__ import annotations

from typing import Any
import logging

from homeassistant import config_entries
from homeassistant.core import HomeAssistant

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class LymowConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Lymow."""

    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None):
        if user_input is None:
            return self.async_show_form(step_id="user", data_schema={})

        # For now we only store host if provided
        data = {"host": user_input.get("host")} if user_input else {}
        return self.async_create_entry(title="Lymow", data=data)
