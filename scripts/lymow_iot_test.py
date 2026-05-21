import json
import os
import time
import base64
from datetime import datetime

from awscrt import mqtt
from awscrt.auth import AwsCredentialsProvider
from awsiot import mqtt_connection_builder
from dotenv import load_dotenv


load_dotenv()


AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
IOT_ENDPOINT = os.getenv("LYMOW_IOT_ENDPOINT")
THING_NAME = os.getenv("LYMOW_THING_NAME")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")


def require_env(name: str, value: str | None) -> str:
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

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

    return {
        "robot_status_code": first_int(1),
        "battery": first_int(2),
        "wifi_signal": unsigned_to_signed_64(wifi_raw) if wifi_raw is not None else None,
        "lte_signal": unsigned_to_signed_64(lte_raw) if lte_raw is not None else None,
        "work_status_code": first_int(6),
        "connection_flag": first_int(10),
    }

def pretty_print_payload(topic: str, payload: bytes) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    text = payload.decode("utf-8", errors="replace")

    print("\n" + "=" * 80)
    print(f"{timestamp}")
    print(f"Topic: {topic}")
    print("-" * 80)

    try:
        parsed = json.loads(text)
        print("JSON:")
        print(json.dumps(parsed, indent=2))

        message = parsed.get("message")
        if message:
            decoded = base64.b64decode(message)

            try:
                robot_info = decode_robot_info(decoded)

                if robot_info:
                    print("\nDecoded robot info:")
                    print(json.dumps(robot_info, indent=2))
                else:
                    print("\nDecoded robot info: not found in this packet")
            except Exception as err:
                print(f"\nDecoded robot info failed: {err}")

            print("\nDecoded binary:")
            print(f"Length: {len(decoded)} bytes")
            print(f"Hex: {decoded.hex()}")

            output_dir = "captures"
            os.makedirs(output_dir, exist_ok=True)

            safe_ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            bin_path = os.path.join(output_dir, f"lymow_{safe_ts}.bin")
            json_path = os.path.join(output_dir, f"lymow_{safe_ts}.json")

            with open(bin_path, "wb") as f:
                f.write(decoded)

            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(parsed, f, indent=2)

            print(f"\nSaved binary: {bin_path}")
            print(f"Saved JSON:   {json_path}")

    except json.JSONDecodeError:
        print(text)
    except Exception as err:
        print(f"Failed to decode payload: {err}")
        print(text)

    print("=" * 80)


def on_message(topic, payload, dup, qos, retain, **kwargs):
    pretty_print_payload(topic, payload)


def main() -> None:
    endpoint = require_env("LYMOW_IOT_ENDPOINT", IOT_ENDPOINT)
    thing_name = require_env("LYMOW_THING_NAME", THING_NAME)

    access_key = require_env("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
    secret_key = require_env("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
    session_token = require_env("AWS_SESSION_TOKEN", AWS_SESSION_TOKEN)

    topics = [
        f"/device/{thing_name}/pboutput",
        f"/device/{thing_name}/notify-app",
    ]

    client_id = f"ha-lymow-test-{int(time.time())}"

    print("HA-Lymow AWS IoT Test Subscriber")
    print("--------------------------------")
    print(f"Region:    {AWS_REGION}")
    print(f"Endpoint:  {endpoint}")
    print(f"Thing:     {thing_name}")
    print(f"Client ID: {client_id}")
    print("Topics:")
    for topic in topics:
        print(f"  - {topic}")

    credentials_provider = AwsCredentialsProvider.new_static(
        access_key_id=access_key,
        secret_access_key=secret_key,
        session_token=session_token,
    )

    mqtt_connection = mqtt_connection_builder.websockets_with_default_aws_signing(
        endpoint=endpoint,
        region=AWS_REGION,
        credentials_provider=credentials_provider,
        client_id=client_id,
        clean_session=False,
        keep_alive_secs=30,
    )

    print("\nConnecting to AWS IoT...")
    connect_future = mqtt_connection.connect()
    connect_future.result()
    print("Connected.")

    for topic in topics:
        print(f"Subscribing to {topic}...")
        subscribe_future, _ = mqtt_connection.subscribe(
            topic=topic,
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_message,
        )
        subscribe_result = subscribe_future.result()
        print(f"Subscribed: {topic}; QoS={subscribe_result['qos']}")

    print("\nListening for Lymow messages. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDisconnecting...")
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        print("Disconnected.")


if __name__ == "__main__":
    main()
