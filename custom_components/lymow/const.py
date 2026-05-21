"""Constants for the Lymow integration."""

from homeassistant.const import CONF_EMAIL, CONF_PASSWORD

DOMAIN = "lymow"
DEFAULT_NAME = "Lymow"
PLATFORMS = ["sensor", "binary_sensor"]

CONF_AWS_REGION = "aws_region"
CONF_COGNITO_REGION = "cognito_region"
CONF_COGNITO_USER_POOL_ID = "cognito_user_pool_id"
CONF_COGNITO_USER_POOL_CLIENT_ID = "cognito_user_pool_client_id"
CONF_COGNITO_IDENTITY_POOL_ID = "cognito_identity_pool_id"
CONF_LYMOW_IOT_ENDPOINT = "lymow_iot_endpoint"
CONF_LYMOW_THING_NAME = "lymow_thing_name"

AWS_REGION_DEFAULT = "us-east-2"
COGNITO_REGION_DEFAULT = "us-east-2"

CONF_EMAIL = CONF_EMAIL
CONF_PASSWORD = CONF_PASSWORD
