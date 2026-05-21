"""Config flow for Lymow integration."""
from __future__ import annotations

from typing import Any
import logging

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant

from .const import (
    DOMAIN,
    CONF_AWS_REGION,
    CONF_COGNITO_REGION,
    CONF_COGNITO_USER_POOL_ID,
    CONF_COGNITO_USER_POOL_CLIENT_ID,
    CONF_COGNITO_IDENTITY_POOL_ID,
    CONF_LYMOW_IOT_ENDPOINT,
    CONF_LYMOW_THING_NAME,
    AWS_REGION_DEFAULT,
    COGNITO_REGION_DEFAULT,
)

_LOGGER = logging.getLogger(__name__)


class LymowConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Lymow."""

    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None):
        if user_input is None:
            return self.async_show_form(
                step_id="user",
                data_schema=vol.Schema(
                    {
                        vol.Required(CONF_LYMOW_THING_NAME): str,
                        vol.Required(CONF_LYMOW_IOT_ENDPOINT): str,
                        vol.Required(CONF_EMAIL): str,
                        vol.Required(CONF_PASSWORD): str,
                        vol.Required(CONF_COGNITO_USER_POOL_ID): str,
                        vol.Required(CONF_COGNITO_USER_POOL_CLIENT_ID): str,
                        vol.Required(CONF_COGNITO_IDENTITY_POOL_ID): str,
                        vol.Required(CONF_AWS_REGION, default=AWS_REGION_DEFAULT): str,
                        vol.Required(CONF_COGNITO_REGION, default=COGNITO_REGION_DEFAULT): str,
                    }
                ),
            )

        return self.async_create_entry(
            title=user_input[CONF_LYMOW_THING_NAME],
            data=user_input,
        )
