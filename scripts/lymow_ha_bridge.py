import base64
import json
import os
import time
from datetime import datetime, timedelta, timezone

import boto3
import paho.mqtt.client as paho_mqtt
from awscrt import mqtt
from awscrt.auth import AwsCredentialsProvider
from awsiot import mqtt_connection_builder
from dotenv import load_dotenv
from pycognito import Cognito


load_dotenv()


AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
IOT_ENDPOINT = os.getenv("LYMOW_IOT_ENDPOINT")
THING_NAME = os.getenv("LYMOW_THING_NAME")

COGNITO_REGION = os.getenv("COGNITO_REGION", "us-east-2")
COGNITO_USER_POOL_ID = os.getenv("COGNITO_USER_POOL_ID")
COGNITO_USER_POOL_CLIENT_ID = os.getenv("COGNITO_USER_POOL_CLIENT_ID")
COGNITO_IDENTITY_POOL_ID = os.getenv("COGNITO_IDENTITY_POOL_ID")
LYMOW_EMAIL = os.getenv("LYMOW_EMAIL")
LYMOW_PASSWORD = os.getenv("LYMOW_PASSWORD")

MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_BASE_TOPIC = os.getenv("MQTT_BASE_TOPIC", "lymow/mowmow")

REFRESH_BUFFER_MINUTES = 5

ROBOT_STATUS_MAP = {
    2: "Mowing",
    3: "Paused",
    4: "Docking",
    5: "Charging",
}


def require_env(name: str, value: str | None) -> str:
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_aws_credentials_from_cognito() -> dict:
    user_pool_id = require_env("COGNITO_USER_POOL_ID", COGNITO_USER_POOL_ID)
    client_id = require_env("COGNITO_USER_POOL_CLIENT_ID", COGNITO_USER_POOL_CLIENT_ID)
    identity_pool_id = require_env("COGNITO_IDENTITY_POOL_ID", COGNITO_IDENTITY_POOL_ID)
    email = require_env("LYMOW_EMAIL", LYMOW_EMAIL)
    password = require_env("LYMOW_PASSWORD", LYMOW_PASSWORD)

    print("Logging into Lymow Cognito with SRP...")

    user = Cognito(
        user_pool_id=user_pool_id,
        client_id=client_id,
        username=email,
        user_pool_region=COGNITO_REGION,
    )

    user.authenticate(password=password)

    id_token = user.id_token
    login_provider = f"cognito-idp.{COGNITO_REGION}.amazonaws.com/{user_pool_id}"

    identity = boto3.client("cognito-identity", region_name=COGNITO_REGION)

    identity_response = identity.get_id(
        IdentityPoolId=identity_pool_id,
        Logins={
            login_provider: id_token,
        },
    )

    identity_id = identity_response["IdentityId"]
    print(f"Cognito Identity ID: {identity_id}")

    creds_response = identity.get_credentials_for_identity(
        IdentityId=identity_id,
        Logins={
            login_provider: id_token,
        },
    )

    creds = creds_response["Credentials"]
    expiration = creds["Expiration"]

    if expiration.tzinfo is None:
        expiration = expiration.replace(tzinfo=timezone.utc)

    print(f"Temporary AWS credentials received. Expiration: {expiration.isoformat()}")

    return {
        "access_key_id": creds["AccessKeyId"],
        "secret_access_key": creds["SecretKey"],
        "session_token": creds["SessionToken"],
        "expiration": expiration,
        "identity_id": identity_id,
    }


def should_refresh_credentials(expiration: datetime) -> bool:
    now = datetime.now(expiration.tzinfo or timezone.utc)
    refresh_at = expiration - timedelta(minutes=REFRESH_BUFFER_MINUTES)
    return now >= refresh_at


def parse_varint(data: bytes, pos: int) -> tuple[int, int]:
    shift = 0
    result = 0

    while True:
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift

        if not (byte & 0x80):
            return result, pos

        shift += 7


def unsigned_to_signed_64(value: int) -> int:
    if value >= 2**63:
        return value - 2**64
    return value


def parse_protobuf_fields(data: bytes) -> dict[int, list]:
    pos = 0
    fields: dict[int, list] = {}

    while pos < len(data):
        key, pos = parse_varint(data, pos)
        field_number = key >> 3
        wire_type = key & 0x07

        if wire_type == 0:
            value, pos = parse_varint(data, pos)
        elif wire_type == 1:
            value = data[pos:pos + 8]
            pos += 8
        elif wire_type == 2:
            length, pos = parse_varint(data, pos)
            value = data[pos:pos + length]
            pos += length
        elif wire_type == 5:
            value = data[pos:pos + 4]
            pos += 4
        else:
            raise ValueError(f"Unsupported protobuf wire type: {wire_type}")

        fields.setdefault(field_number, []).append(value)

    return fields


def decode_robot_info(decoded: bytes) -> dict | None:
    top_fields = parse_protobuf_fields(decoded)

    if 5 not in top_fields:
        return None

    robot_blob = top_fields[5][0]

    if not isinstance(robot_blob, bytes):
        return None

    robot_fields = parse_protobuf_fields(robot_blob)

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
        "wifi_signal": unsigned_to_signed_64(wifi_raw) if wifi_raw is not None else None,
        "lte_signal": unsigned_to_signed_64(lte_raw) if lte_raw is not None else None,
        "work_status_code": work_status_code,
        "connection_flag": first_int(10),
        "last_update": datetime.now().isoformat(timespec="seconds"),
    }


def publish_discovery(mqtt_client: paho_mqtt.Client) -> None:
    device = {
        "identifiers": [f"lymow_{THING_NAME}"],
        "name": "Lymow MowMow",
        "manufacturer": "Lymow",
        "model": "Lymow One Plus",
    }

    sensors = [
        {
            "object_id": "lymow_battery",
            "name": "Lymow Battery",
            "state_topic": f"{MQTT_BASE_TOPIC}/state",
            "value_template": "{{ value_json.battery }}",
            "unit_of_measurement": "%",
            "device_class": "battery",
            "state_class": "measurement",
            "icon": "mdi:battery",
        },
        {
            "object_id": "lymow_robot_status",
            "name": "Lymow Robot Status",
            "state_topic": f"{MQTT_BASE_TOPIC}/state",
            "value_template": "{{ value_json.robot_status }}",
            "icon": "mdi:robot-mower",
        },
        {
            "object_id": "lymow_robot_status_code",
            "name": "Lymow Robot Status Code",
            "state_topic": f"{MQTT_BASE_TOPIC}/state",
            "value_template": "{{ value_json.robot_status_code }}",
            "icon": "mdi:robot-mower",
        },
        {
            "object_id": "lymow_work_status_code",
            "name": "Lymow Work Status Code",
            "state_topic": f"{MQTT_BASE_TOPIC}/state",
            "value_template": "{{ value_json.work_status_code }}",
            "icon": "mdi:state-machine",
        },
        {
            "object_id": "lymow_wifi_signal",
            "name": "Lymow WiFi Signal",
            "state_topic": f"{MQTT_BASE_TOPIC}/state",
            "value_template": "{{ value_json.wifi_signal }}",
            "unit_of_measurement": "dBm",
            "device_class": "signal_strength",
            "state_class": "measurement",
            "icon": "mdi:wifi",
        },
        {
            "object_id": "lymow_lte_signal",
            "name": "Lymow LTE Signal",
            "state_topic": f"{MQTT_BASE_TOPIC}/state",
            "value_template": "{{ value_json.lte_signal }}",
            "unit_of_measurement": "dBm",
            "device_class": "signal_strength",
            "state_class": "measurement",
            "icon": "mdi:signal-4g",
        },
        {
            "object_id": "lymow_last_update",
            "name": "Lymow Last Update",
            "state_topic": f"{MQTT_BASE_TOPIC}/state",
            "value_template": "{{ value_json.last_update }}",
            "icon": "mdi:clock-outline",
        },
    ]

    binary_sensors = [
        {
            "object_id": "lymow_connected",
            "name": "Lymow Connected",
            "state_topic": f"{MQTT_BASE_TOPIC}/state",
            "value_template": "{{ 'ON' if value_json.connection_flag == 1 else 'OFF' }}",
            "payload_on": "ON",
            "payload_off": "OFF",
            "device_class": "connectivity",
        }
    ]

    for sensor in sensors:
        object_id = sensor["object_id"]
        topic = f"homeassistant/sensor/{object_id}/config"
        payload = {
            "unique_id": object_id,
            **sensor,
            "device": device,
            "availability_topic": f"{MQTT_BASE_TOPIC}/availability",
            "payload_available": "online",
            "payload_not_available": "offline",
        }
        mqtt_client.publish(topic, json.dumps(payload), retain=True)

    for sensor in binary_sensors:
        object_id = sensor["object_id"]
        topic = f"homeassistant/binary_sensor/{object_id}/config"
        payload = {
            "unique_id": object_id,
            **sensor,
            "device": device,
            "availability_topic": f"{MQTT_BASE_TOPIC}/availability",
            "payload_available": "online",
            "payload_not_available": "offline",
        }
        mqtt_client.publish(topic, json.dumps(payload), retain=True)


def create_ha_mqtt_client() -> paho_mqtt.Client:
    mqtt_host = require_env("MQTT_HOST", MQTT_HOST)

    client = paho_mqtt.Client(client_id=f"ha-lymow-bridge-{int(time.time())}")

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    print(f"Connecting to Home Assistant MQTT: {mqtt_host}:{MQTT_PORT}")
    client.connect(mqtt_host, MQTT_PORT, keepalive=60)
    client.loop_start()

    client.publish(f"{MQTT_BASE_TOPIC}/availability", "online", retain=True)
    publish_discovery(client)

    return client


def build_aws_connection(credentials: dict, client_id: str):
    credentials_provider = AwsCredentialsProvider.new_static(
        access_key_id=credentials["access_key_id"],
        secret_access_key=credentials["secret_access_key"],
        session_token=credentials["session_token"],
    )

    return mqtt_connection_builder.websockets_with_default_aws_signing(
        endpoint=require_env("LYMOW_IOT_ENDPOINT", IOT_ENDPOINT),
        region=AWS_REGION,
        credentials_provider=credentials_provider,
        client_id=client_id,
        clean_session=False,
        keep_alive_secs=30,
    )


def handle_aws_message(ha_mqtt_client: paho_mqtt.Client, topic, payload, dup, qos, retain, **kwargs):
    text = payload.decode("utf-8", errors="replace")

    try:
        parsed = json.loads(text)
        message = parsed.get("message")
        if not message:
            return

        decoded = base64.b64decode(message)
        robot_info = decode_robot_info(decoded)

        if not robot_info:
            return

        print("Publishing robot info to HA MQTT:")
        print(json.dumps(robot_info, indent=2))

        ha_mqtt_client.publish(
            f"{MQTT_BASE_TOPIC}/state",
            json.dumps(robot_info),
            retain=True,
        )
        ha_mqtt_client.publish(
            f"{MQTT_BASE_TOPIC}/availability",
            "online",
            retain=True,
        )

    except Exception as err:
        print(f"Failed to process AWS message: {err}")


def connect_and_subscribe_to_aws(ha_mqtt_client: paho_mqtt.Client, credentials: dict):
    thing_name = require_env("LYMOW_THING_NAME", THING_NAME)
    client_id = f"ha-lymow-bridge-{int(time.time())}"

    aws_connection = build_aws_connection(credentials, client_id)

    topics = [
        f"/device/{thing_name}/pboutput",
        f"/device/{thing_name}/notify-app",
    ]

    print("\nConnecting to Lymow AWS IoT...")
    aws_connection.connect().result()
    print("Connected to Lymow AWS IoT.")

    def callback(topic, payload, dup, qos, retain, **kwargs):
        handle_aws_message(ha_mqtt_client, topic, payload, dup, qos, retain, **kwargs)

    for topic in topics:
        print(f"Subscribing to {topic}...")
        subscribe_future, _ = aws_connection.subscribe(
            topic=topic,
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=callback,
        )
        result = subscribe_future.result()
        print(f"Subscribed: {topic}; QoS={result['qos']}")

    return aws_connection


def main() -> None:
    endpoint = require_env("LYMOW_IOT_ENDPOINT", IOT_ENDPOINT)
    thing_name = require_env("LYMOW_THING_NAME", THING_NAME)

    print("HA-Lymow Home Assistant Bridge")
    print("------------------------------")
    print(f"AWS region:   {AWS_REGION}")
    print(f"AWS endpoint: {endpoint}")
    print(f"Thing:        {thing_name}")
    print(f"MQTT base:    {MQTT_BASE_TOPIC}")

    ha_mqtt_client = create_ha_mqtt_client()

    credentials = get_aws_credentials_from_cognito()
    aws_connection = connect_and_subscribe_to_aws(ha_mqtt_client, credentials)

    print("\nBridge running. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(10)

            if should_refresh_credentials(credentials["expiration"]):
                print("\nAWS credentials are near expiration. Refreshing and reconnecting...")

                try:
                    ha_mqtt_client.publish(f"{MQTT_BASE_TOPIC}/availability", "offline", retain=True)

                    try:
                        aws_connection.disconnect().result()
                        print("Disconnected old AWS IoT connection.")
                    except Exception as err:
                        print(f"Old AWS disconnect warning: {err}")

                    credentials = get_aws_credentials_from_cognito()
                    aws_connection = connect_and_subscribe_to_aws(ha_mqtt_client, credentials)

                    ha_mqtt_client.publish(f"{MQTT_BASE_TOPIC}/availability", "online", retain=True)
                    print("Credential refresh and AWS reconnect complete.")

                except Exception as err:
                    print(f"Credential refresh/reconnect failed: {err}")
                    print("Will retry in 60 seconds.")
                    time.sleep(60)

    except KeyboardInterrupt:
        print("\nStopping bridge...")
        ha_mqtt_client.publish(f"{MQTT_BASE_TOPIC}/availability", "offline", retain=True)

        try:
            aws_connection.disconnect().result()
        except Exception as err:
            print(f"AWS disconnect warning: {err}")

        ha_mqtt_client.loop_stop()
        ha_mqtt_client.disconnect()
        print("Stopped.")


if __name__ == "__main__":
    main()