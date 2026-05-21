"""AWS IoT hub/client for Lymow device interactions."""
from __future__ import annotations

import base64
import json
import logging
import os
import struct
from datetime import datetime, timedelta, timezone
from threading import Event
from typing import Any, Callable

import boto3
from awscrt import mqtt
from awscrt.auth import AwsCredentialsProvider
from awsiot import mqtt_connection_builder
from homeassistant.core import HomeAssistant
from pycognito import Cognito

from .const import (
    AWS_REGION_DEFAULT,
    COGNITO_REGION_DEFAULT,
    CONF_AWS_REGION,
    CONF_COGNITO_IDENTITY_POOL_ID,
    CONF_COGNITO_REGION,
    CONF_COGNITO_USER_POOL_CLIENT_ID,
    CONF_COGNITO_USER_POOL_ID,
    CONF_EMAIL,
    CONF_LYMOW_IOT_ENDPOINT,
    CONF_LYMOW_THING_NAME,
    CONF_PASSWORD,
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
        """Connect to Lymow AWS IoT."""
        _LOGGER.debug("Connecting Lymow hub")
        await self.hass.async_add_executor_job(self._connect_sync)
        self._refresh_task = self.hass.async_create_task(self._async_refresh_loop())

    async def async_disconnect(self) -> None:
        """Disconnect from Lymow AWS IoT."""
        _LOGGER.debug("Disconnecting Lymow hub")

        self._stop_event.set()

        if self._refresh_task:
            self._refresh_task.cancel()
            self._refresh_task = None

        await self.hass.async_add_executor_job(self._disconnect_sync)
        self._connected = False

    async def async_get_state(self) -> dict[str, Any]:
        """Return current Lymow state."""
        return self.state

    def async_register_listener(self, listener: Callable[[], None]) -> None:
        """Register a listener for state changes."""
        if listener not in self._listeners:
            self._listeners.append(listener)

    def async_remove_listener(self, listener: Callable[[], None]) -> None:
        """Remove a registered listener."""
        if listener in self._listeners:
            self._listeners.remove(listener)

    @property
    def available(self) -> bool:
        """Return whether the hub is connected."""
        return self._connected

    def _connect_sync(self) -> None:
        """Connect to AWS IoT."""
        self._credentials = self._fetch_aws_credentials()
        self._aws_connection = self._build_aws_connection(self._credentials)

        _LOGGER.debug(
            "Connecting to Lymow AWS IoT endpoint: %s",
            self.config[CONF_LYMOW_IOT_ENDPOINT],
        )
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

        self._aws_connection = None

    def _fetch_aws_credentials(self) -> dict[str, Any]:
        """Authenticate with Cognito and fetch temporary AWS credentials."""
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
        """Build AWS IoT MQTT websocket connection."""
        client_id = f"ha-lymow-{int(datetime.now(timezone.utc).timestamp())}"

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
            clean_session=True,
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
        """Handle incoming AWS IoT MQTT messages."""
        try:
            text = payload.decode("utf-8", errors="replace")
            parsed = json.loads(text)

            message = parsed.get("message")
            if not message:
                return

            decoded = base64.b64decode(message)

            _LOGGER.info(
                "Lymow MQTT message received: topic=%s decoded_size=%s bytes",
                topic,
                len(decoded),
            )

            map_data = self._decode_map_info(decoded)

            if not map_data and len(decoded) > 500:
                _LOGGER.info(
                    "Lymow larger payload received but no map decoded: size=%s bytes",
                    len(decoded),
                )

            if map_data:
                map_url = self._save_map_data(map_data)

                self.state["map_loaded"] = True
                self.state["map_zone_count"] = map_data["zone_count"]
                self.state["map_point_count"] = map_data["point_count"]
                self.state["map_updated_at"] = map_data["updated_at"]
                self.state["map_json_url"] = map_url

                _LOGGER.info(
                    "Decoded Lymow map: %s zones, %s points, url=%s",
                    map_data["zone_count"],
                    map_data["point_count"],
                    map_url,
                )

                self._notify_listeners()

            robot_info = self._decode_robot_info(decoded)
            if not robot_info:
                return

            pose_info = self._decode_pose_info(decoded)
            robot_info.update(pose_info)

            if self._has_meaningful_state_change(robot_info):
                self.state.update(robot_info)
                self.state["last_update"] = datetime.now(timezone.utc).isoformat(
                    timespec="seconds"
                )
                self._notify_listeners()
            else:
                _LOGGER.debug("Ignoring Lymow message with no meaningful state change")

        except Exception:
            _LOGGER.exception("Failed to process AWS message")

    def _has_meaningful_state_change(self, new_state: dict[str, Any]) -> bool:
        """Return true if the new Lymow state changed in a meaningful way."""
        keys_to_compare = [
            "robot_status_code",
            "robot_status",
            "battery",
            "wifi_signal",
            "lte_signal",
            "work_status_code",
            "connection_flag",
            "map_x",
            "map_y",
            "heading_radians",
            "heading_degrees",
        ]

        for key in keys_to_compare:
            if self.state.get(key) != new_state.get(key):
                return True

        return False

    def _notify_listeners(self) -> None:
        """Notify Home Assistant entities that state changed."""
        for listener in list(self._listeners):
            self.hass.add_job(listener)

    async def _async_refresh_loop(self) -> None:
        """Refresh AWS credentials before expiration."""
        while True:
            await self.hass.async_add_executor_job(self._stop_event.wait, 10)

            if self._stop_event.is_set():
                return

            if self._credentials and self._should_refresh_credentials(
                self._credentials["expiration"]
            ):
                self._logger_refresh()
                await self.hass.async_add_executor_job(self._reconnect_sync)

    def _logger_refresh(self) -> None:
        """Log credential refresh."""
        _LOGGER.info("Refreshing Lymow AWS credentials")

    def _reconnect_sync(self) -> None:
        """Reconnect to AWS IoT with refreshed credentials."""
        try:
            self._disconnect_sync()

            self._credentials = self._fetch_aws_credentials()
            self._aws_connection = self._build_aws_connection(self._credentials)

            self._aws_connection.connect().result(timeout=30)
            self._subscribe_to_topics()

            self._connected = True

        except Exception:
            self._connected = False
            _LOGGER.exception("Lymow AWS reconnect failed")

    @staticmethod
    def _should_refresh_credentials(expiration: datetime) -> bool:
        """Return true if AWS credentials should be refreshed."""
        refresh_at = expiration - timedelta(minutes=REFRESH_BUFFER_MINUTES)
        return datetime.now(expiration.tzinfo or timezone.utc) >= refresh_at

    @staticmethod
    def _parse_varint(data: bytes, pos: int) -> tuple[int, int]:
        """Parse a protobuf varint."""
        shift = 0
        result = 0

        while True:
            if pos >= len(data):
                raise ValueError("Unexpected end of protobuf while parsing varint")

            byte = data[pos]
            pos += 1
            result |= (byte & 0x7F) << shift

            if not (byte & 0x80):
                return result, pos

            shift += 7

    @staticmethod
    def _unsigned_to_signed_64(value: int) -> int:
        """Convert unsigned 64-bit integer to signed."""
        if value >= 2**63:
            return value - 2**64
        return value

    @staticmethod
    def _fixed32_to_float(value: bytes) -> float | None:
        """Convert protobuf fixed32 bytes to little-endian float."""
        if not isinstance(value, bytes) or len(value) != 4:
            return None

        return struct.unpack("<f", value)[0]

    def _parse_protobuf_fields(self, data: bytes) -> dict[int, list[Any]]:
        """Parse protobuf fields into a simple field-number dictionary."""
        pos = 0
        fields: dict[int, list[Any]] = {}

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
        """Decode known robot status fields from the protobuf payload."""
        try:
            top_fields = self._parse_protobuf_fields(decoded)
        except Exception:
            return None

        if 5 not in top_fields:
            return None

        robot_blob = top_fields[5][0]

        if not isinstance(robot_blob, bytes):
            return None

        try:
            robot_fields = self._parse_protobuf_fields(robot_blob)
        except Exception:
            return None

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
            "robot_status": ROBOT_STATUS_MAP.get(
                robot_status_code,
                f"Unknown ({robot_status_code})",
            ),
            "battery": first_int(2),
            "wifi_signal": self._unsigned_to_signed_64(wifi_raw)
            if wifi_raw is not None
            else None,
            "lte_signal": self._unsigned_to_signed_64(lte_raw)
            if lte_raw is not None
            else None,
            "work_status_code": work_status_code,
            "connection_flag": first_int(10),
        }

    def _decode_pose_info(self, decoded: bytes) -> dict[str, Any]:
        """Decode live mower local map position and heading."""
        pose_info: dict[str, Any] = {}

        try:
            top_fields = self._parse_protobuf_fields(decoded)
        except Exception:
            return pose_info

        pose_blobs = top_fields.get(14)
        if not pose_blobs:
            return pose_info

        pose_blob = pose_blobs[0]
        if not isinstance(pose_blob, bytes):
            return pose_info

        try:
            pose_fields = self._parse_protobuf_fields(pose_blob)
        except Exception:
            return pose_info

        def first_float(field_num: int) -> float | None:
            values = pose_fields.get(field_num)
            if not values:
                return None

            return self._fixed32_to_float(values[0])

        map_x = first_float(1)
        map_y = first_float(2)
        heading = first_float(3)

        if map_x is not None:
            pose_info["map_x"] = round(map_x, 4)

        if map_y is not None:
            pose_info["map_y"] = round(map_y, 4)

        if heading is not None:
            pose_info["heading_radians"] = round(heading, 4)
            pose_info["heading_degrees"] = round(
                heading * 180 / 3.141592653589793,
                2,
            )

        return pose_info

    def _decode_point(self, point_blob: bytes) -> dict[str, float] | None:
        """Decode a single map point from protobuf bytes."""
        try:
            point_fields = self._parse_protobuf_fields(point_blob)
        except Exception:
            return None

        x_values = point_fields.get(1)
        y_values = point_fields.get(2)

        if not x_values or not y_values:
            return None

        x = self._fixed32_to_float(x_values[0])
        y = self._fixed32_to_float(y_values[0])

        if x is None or y is None:
            return None

        return {
            "x": round(x, 4),
            "y": round(y, 4),
        }

    def _decode_zone_record(self, zone_blob: bytes) -> dict[str, Any] | None:
        """Decode one Lymow map zone record."""
        try:
            zone_fields = self._parse_protobuf_fields(zone_blob)
        except Exception:
            return None

        name = None
        zone_id = None
        points: list[dict[str, float]] = []

        header_values = zone_fields.get(1)
        if header_values and isinstance(header_values[0], bytes):
            try:
                header_fields = self._parse_protobuf_fields(header_values[0])

                name_values = header_fields.get(2)
                id_values = header_fields.get(3)

                if name_values and isinstance(name_values[0], bytes):
                    name = name_values[0].decode("utf-8", errors="replace")

                if id_values and isinstance(id_values[0], bytes):
                    zone_id = id_values[0].decode("utf-8", errors="replace")

            except Exception:
                _LOGGER.debug("Failed to decode Lymow map zone header", exc_info=True)

        point_values = zone_fields.get(5, [])
        for point_blob in point_values:
            if not isinstance(point_blob, bytes):
                continue

            point = self._decode_point(point_blob)
            if point:
                points.append(point)

        if not name and not zone_id and not points:
            return None

        return {
            "name": name or "Unknown",
            "id": zone_id,
            "point_count": len(points),
            "points": points,
        }

    def _find_zone_records_recursive(
        self,
        blob: bytes,
        depth: int = 0,
        max_depth: int = 6,
    ) -> list[dict[str, Any]]:
        """Find zone records inside nested protobuf data."""
        if depth > max_depth:
            return []

        zones: list[dict[str, Any]] = []

        try:
            fields = self._parse_protobuf_fields(blob)
        except Exception:
            return zones

        if 1 in fields and 5 in fields:
            zone = self._decode_zone_record(blob)
            if zone and zone["point_count"] > 2:
                zones.append(zone)

        for values in fields.values():
            for value in values:
                if isinstance(value, bytes) and len(value) > 0:
                    zones.extend(
                        self._find_zone_records_recursive(
                            value,
                            depth=depth + 1,
                            max_depth=max_depth,
                        )
                    )

        return zones

    def _dedupe_map_zones(self, zones: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Remove duplicate zone records discovered through recursive parsing."""
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str | None, int]] = set()

        for zone in zones:
            key = (
                zone.get("name", "Unknown"),
                zone.get("id"),
                zone.get("point_count", 0),
            )

            if key in seen:
                continue

            seen.add(key)
            deduped.append(zone)

        return deduped

    def _decode_map_info(self, decoded: bytes) -> dict[str, Any] | None:
        """Decode Lymow map/zone data from a large pboutput payload."""
        try:
            top_fields = self._parse_protobuf_fields(decoded)
        except Exception:
            return None

        map_blobs = top_fields.get(23)
        if not map_blobs:
            return None

        zones: list[dict[str, Any]] = []

        for map_blob in map_blobs:
            if not isinstance(map_blob, bytes):
                continue

            zones.extend(self._find_zone_records_recursive(map_blob))

        zones = self._dedupe_map_zones(zones)

        if not zones:
            return None

        total_points = sum(zone.get("point_count", 0) for zone in zones)

        return {
            "thing_name": self.config.get(CONF_LYMOW_THING_NAME),
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "zone_count": len(zones),
            "point_count": total_points,
            "zones": zones,
        }

    def _save_map_data(self, map_data: dict[str, Any]) -> str | None:
        """Save decoded map data to /config/www/lymow for Lovelace/local access."""
        try:
            thing_name = self.config.get(CONF_LYMOW_THING_NAME, "lymow")
            safe_thing_name = "".join(
                char if char.isalnum() or char in ("_", "-") else "_"
                for char in thing_name
            )

            map_dir = self.hass.config.path("www", "lymow")
            os.makedirs(map_dir, exist_ok=True)

            filename = f"{safe_thing_name}_map.json"
            path = os.path.join(map_dir, filename)

            with open(path, "w", encoding="utf-8") as file:
                json.dump(map_data, file, indent=2)

            return f"/local/lymow/{filename}"

        except Exception:
            _LOGGER.exception("Failed saving Lymow map data")
            return None