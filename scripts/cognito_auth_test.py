import json
import os
from datetime import timezone

import boto3
from dotenv import load_dotenv
from pycognito import Cognito


load_dotenv()


REGION = os.getenv("COGNITO_REGION", "us-east-2")
USER_POOL_ID = os.getenv("COGNITO_USER_POOL_ID")
CLIENT_ID = os.getenv("COGNITO_USER_POOL_CLIENT_ID")
IDENTITY_POOL_ID = os.getenv("COGNITO_IDENTITY_POOL_ID")

EMAIL = os.getenv("LYMOW_EMAIL")
PASSWORD = os.getenv("LYMOW_PASSWORD")


def require_env(name: str, value: str | None) -> str:
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> None:
    user_pool_id = require_env("COGNITO_USER_POOL_ID", USER_POOL_ID)
    client_id = require_env("COGNITO_USER_POOL_CLIENT_ID", CLIENT_ID)
    identity_pool_id = require_env("COGNITO_IDENTITY_POOL_ID", IDENTITY_POOL_ID)
    email = require_env("LYMOW_EMAIL", EMAIL)
    password = require_env("LYMOW_PASSWORD", PASSWORD)

    print("Logging into Cognito User Pool with SRP...")

    user = Cognito(
        user_pool_id=user_pool_id,
        client_id=client_id,
        username=email,
        user_pool_region=REGION,
    )

    user.authenticate(password=password)

    id_token = user.id_token
    access_token = user.access_token

    print("Login successful.")
    print(f"Access token length: {len(access_token)}")
    print(f"ID token length: {len(id_token)}")

    login_provider = f"cognito-idp.{REGION}.amazonaws.com/{user_pool_id}"

    identity = boto3.client("cognito-identity", region_name=REGION)

    print("Getting Cognito Identity ID...")

    identity_response = identity.get_id(
        IdentityPoolId=identity_pool_id,
        Logins={
            login_provider: id_token,
        },
    )

    identity_id = identity_response["IdentityId"]
    print(f"Identity ID: {identity_id}")

    print("Getting temporary AWS credentials...")

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

    print("Temporary AWS credentials received:")
    print(json.dumps({
        "AccessKeyId_present": bool(creds.get("AccessKeyId")),
        "SecretKey_present": bool(creds.get("SecretKey")),
        "SessionToken_present": bool(creds.get("SessionToken")),
        "Expiration": expiration.isoformat(),
    }, indent=2))


if __name__ == "__main__":
    main()