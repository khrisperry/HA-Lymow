"""AWS IoT hub/client for Lymow device interactions."""
from __future__ import annotations

import base64
import json
import logging
from datetime import datetime, timedelta, timezone
from threading import Event
from typing import Any, Callable

import boto3
from awscrt import mqtt
from awscrt.auth import AwsCredentialsProvider
from awsiot import mqtt_connection_builder
from pycognito import Cognito
from homeassistant.core import HomeAssistant

from .const import (
    AWS_REGION_DEFAULT,
    COGNITO_REGION_DEFAULT,
    CONF_AWS_REGION,
    CONF_COGNITO_REGION,
    CONF_COGNITO_IDENTITY_POOL_ID,
    CONF_COGNITO_USER_POOL_CLIENT_ID,
    CONF_COGNITO_USER_POOL_ID,
    CONF_EMAIL,
    CONF_PASSWORD,
    CONF_LYMOW_IOT_ENDPOINT,
    CONF_LYMOW_THING_NAME,
)

_LOGGER = logging.getLogger(__name__)
REFRESH_BUFFER_MINUTES = 5
ROBOT_STATUS_MAP = {
    2: "Mowing",
    3: "Paused",
    4: "Docking",
    5: "Charging",
}


class LymowHub:
    """Hub for Lymow AWS IoT data and state."""

    def __init__(self, hass: HomeAssistant, config: dict[str, Any]) -> None:
        self.hass = hass
        self.config = config
        self.state: dict[str, Any] = {}
        self._listeners: list[Callable[[], None]] = []
        self._connected = False
        self._aws_connection = None
        self._credentials: dict[str, Any] | None = None
        self._refresh_task = None
        self._stop_event = Event()

    async def async_connect(self) -> None:
        _LOGGER.debug("Connecting Lymow hub")
        await self.hass.async_add_executor_job(self._connect_sync)
        self._refresh_task = self.hass.async_create_task(self._async_refresh_loop())

    async def async_disconnect(self) -> None:
        _LOGGER.debug("Disconnecting Lymow hub")
        if self._refresh_task:
            self._refresh_task.cancel()

        await self.hass.async_add_executor_job(self._disconnect_sync)
        self._connected = False

    async def async_get_state(self) -> dict[str, Any]:
        return self.state

    def async_register_listener(self, listener: Callable[[], None]) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def async_remove_listener(self, listener: Callable[[], None]) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    @property
    def available(self) -> bool:
        return self._connected

    def _connect_sync(self) -> None:
        """Connect to AWS IoT."""
        self._credentials = self._fetch_aws_credentials()
        self._aws_connection = self._build_aws_connection(self._credentials)

        _LOGGER.debug("Connecting to Lymow AWS IoT endpoint: %s", self.config[CONF_LYMOW_IOT_ENDPOINT])
        self._aws_connection.connect().result(timeout=30)
        _LOGGER.debug("Connected to Lymow AWS IoT endpoint")

        self._subscribe_to_topics()

        self._connected = True
        _LOGGER.info("Lymow AWS IoT connected")


    def _disconnect_sync(self) -> None:
        """Disconnect from AWS IoT."""
        if self._aws_connection is not None:
            try:
                self._aws_connection.disconnect().result(timeout=10)
            except Exception as err:
                _LOGGER.warning("Lymow AWS disconnect failed: %s", err)

    def _fetch_aws_credentials(self) -> dict[str, Any]:
        region = self.config.get(CONF_COGNITO_REGION, COGNITO_REGION_DEFAULT)
        user_pool_id = self.config[CONF_COGNITO_USER_POOL_ID]
        client_id = self.config[CONF_COGNITO_USER_POOL_CLIENT_ID]
        identity_pool_id = self.config[CONF_COGNITO_IDENTITY_POOL_ID]
        email = self.config[CONF_EMAIL]
        password = self.config[CONF_PASSWORD]

        user = Cognito(
            user_pool_id=user_pool_id,
            client_id=client_id,
            username=email,
            user_pool_region=region,
        )
        user.authenticate(password=password)

        login_provider = f"cognito-idp.{region}.amazonaws.com/{user_pool_id}"
        identity = boto3.client("cognito-identity", region_name=region)
        identity_response = identity.get_id(
            IdentityPoolId=identity_pool_id,
            Logins={login_provider: user.id_token},
        )
        creds_response = identity.get_credentials_for_identity(
            IdentityId=identity_response["IdentityId"],
            Logins={login_provider: user.id_token},
        )
        creds = creds_response["Credentials"]
        expiration = creds["Expiration"]
        if expiration.tzinfo is None:
            expiration = expiration.replace(tzinfo=timezone.utc)

        return {
            "access_key_id": creds["AccessKeyId"],
            "secret_access_key": creds["SecretKey"],
            "session_token": creds["SessionToken"],
            "expiration": expiration,
        }

    def _build_aws_connection(self, credentials: dict[str, Any]):
        client_id = f"ha-lymow-{int(datetime.now().timestamp())}"
        credentials_provider = AwsCredentialsProvider.new_static(
            access_key_id=credentials["access_key_id"],
            secret_access_key=credentials["secret_access_key"],
            session_token=credentials["session_token"],
        )
        return mqtt_connection_builder.websockets_with_default_aws_signing(
            endpoint=self.config[CONF_LYMOW_IOT_ENDPOINT],
            region=self.config.get(CONF_AWS_REGION, AWS_REGION_DEFAULT),
            credentials_provider=credentials_provider,
            client_id=client_id,
            clean_session=False,
            keep_alive_secs=30,
        )

    def _subscribe_to_topics(self) -> None:
        """Subscribe to Lymow AWS IoT topics."""
        thing_name = self.config[CONF_LYMOW_THING_NAME]
        topics = [
            f"/device/{thing_name}/pboutput",
            f"/device/{thing_name}/notify-app",
        ]

        for topic in topics:
            _LOGGER.debug("Subscribing to Lymow AWS topic: %s", topic)
            subscribe_future, packet_id = self._aws_connection.subscribe(
                topic=topic,
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=self._handle_aws_message,
            )

            subscribe_result = subscribe_future.result(timeout=30)
            _LOGGER.debug(
                "Subscribed to Lymow AWS topic: %s, packet_id=%s, result=%s",
                topic,
                packet_id,
                subscribe_result,
            )

    def _handle_aws_message(self, topic, payload, dup, qos, retain, **kwargs) -> None:
        try:
            text = payload.decode("utf-8", errors="replace")
            parsed = json.loads(text)
            message = parsed.get("message")
            if not message:
                return
            decoded = base64.b64decode(message)
            robot_info = self._decode_robot_info(decoded)
            if not robot_info:
                return
            self.state.update(robot_info)
            self._notify_listeners()
        except Exception as err:
            _LOGGER.warning("Failed to process AWS message: %s", err)

    def _notify_listeners(self) -> None:
        for listener in list(self._listeners):
            self.hass.add_job(listener)

    async def _async_refresh_loop(self) -> None:
        while True:
            await self.hass.async_add_executor_job(self._stop_event.wait, 10)
            if self._stop_event.is_set():
                return
            if self._credentials and self._should_refresh_credentials(self._credentials["expiration"]):
                self._logger_refresh()
                await self.hass.async_add_executor_job(self._reconnect_sync)

    def _logger_refresh(self) -> None:
        _LOGGER.info("Refreshing Lymow AWS credentials")

    def _reconnect_sync(self) -> None:
        try:
            self._disconnect_sync()
            self._credentials = self._fetch_aws_credentials()
            self._aws_connection = self._build_aws_connection(self._credentials)
            self._aws_connection.connect().result()
            self._subscribe_to_topics()
            self._connected = True
        except Exception as err:
            _LOGGER.warning("Lymow AWS reconnect failed: %s", err)

    @staticmethod
    def _should_refresh_credentials(expiration: datetime) -> bool:
        refresh_at = expiration - timedelta(minutes=REFRESH_BUFFER_MINUTES)
        return datetime.now(expiration.tzinfo or timezone.utc) >= refresh_at

    @staticmethod
    def _parse_varint(data: bytes, pos: int) -> tuple[int, int]:
        shift = 0
        result = 0
        while True:
            byte = data[pos]
            pos += 1
            result |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                return result, pos
            shift += 7

    @staticmethod
    def _unsigned_to_signed_64(value: int) -> int:
        if value >= 2**63:
            return value - 2**64
        return value

    def _parse_protobuf_fields(self, data: bytes) -> dict[int, list]:
        pos = 0
        fields: dict[int, list] = {}
        while pos < len(data):
            key, pos = self._parse_varint(data, pos)
            field_number = key >> 3
            wire_type = key & 0x07
            if wire_type == 0:
                value, pos = self._parse_varint(data, pos)
            elif wire_type == 1:
                value = data[pos : pos + 8]
                pos += 8
            elif wire_type == 2:
                length, pos = self._parse_varint(data, pos)
                value = data[pos : pos + length]
                pos += length
            elif wire_type == 5:
                value = data[pos : pos + 4]
                pos += 4
            else:
                raise ValueError(f"Unsupported protobuf wire type: {wire_type}")
            fields.setdefault(field_number, []).append(value)
        return fields

    def _decode_robot_info(self, decoded: bytes) -> dict[str, Any] | None:
        top_fields = self._parse_protobuf_fields(decoded)
        if 5 not in top_fields:
            return None
        robot_blob = top_fields[5][0]
        if not isinstance(robot_blob, bytes):
            return None
        robot_fields = self._parse_protobuf_fields(robot_blob)

        def first_int(field_num: int):
            values = robot_fields.get(field_num)
            if not values:
                return None
            value = values[0]
            return value if isinstance(value, int) else None

        wifi_raw = first_int(3)
        lte_raw = first_int(4)
        robot_status_code = first_int(1)
        work_status_code = first_int(6)

        return {
            "robot_status_code": robot_status_code,
            "robot_status": ROBOT_STATUS_MAP.get(robot_status_code, f"Unknown ({robot_status_code})"),
            "battery": first_int(2),
            "wifi_signal": self._unsigned_to_signed_64(wifi_raw) if wifi_raw is not None else None,
            "lte_signal": self._unsigned_to_signed_64(lte_raw) if lte_raw is not None else None,
            "work_status_code": work_status_code,
            "connection_flag": first_int(10),
            "last_update": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
