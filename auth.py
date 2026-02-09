"""
Authentication module for App Store Connect API.

This module handles JWT token generation for authenticating with the
App Store Connect API. Tokens are valid for 20 minutes.

Usage:
    from auth import generate_token
    token = generate_token()
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import jwt
from dotenv import load_dotenv

# Load environment variables
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")


def _require_env(name: str) -> str:
    """Get required environment variable or exit with error."""
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Environment variable {name} is not set. Check your .env file.")
    return value


def _load_private_key(path: str) -> str:
    """Load private key from file."""
    key_path = Path(path)
    if not key_path.is_absolute():
        key_path = PROJECT_ROOT / key_path
    if not key_path.exists():
        raise SystemExit(f"Private key file not found: {key_path}")
    return key_path.read_text()


def generate_token() -> str:
    """
    Generate a JWT token for App Store Connect API authentication.

    The token is valid for 20 minutes and uses ES256 algorithm.

    Required environment variables:
        - ISSUER_ID: Your App Store Connect Issuer ID
        - KEY_ID: Your API Key ID
        - PRIVATE_KEY_PATH: Path to your .p8 private key file

    Returns:
        str: The generated JWT token

    Raises:
        SystemExit: If required environment variables are missing
    """
    issuer = _require_env("ISSUER_ID")
    key_id = _require_env("KEY_ID")
    private_key_path = _require_env("PRIVATE_KEY_PATH")
    private_key = _load_private_key(private_key_path)

    payload = {
        "iss": issuer,
        "iat": int(time.time()),
        "exp": int(time.time()) + 1200,  # 20 minutes
        "aud": "appstoreconnect-v1",
    }
    headers = {"alg": "ES256", "kid": key_id, "typ": "JWT"}

    token = jwt.encode(payload, private_key, algorithm="ES256", headers=headers)
    if isinstance(token, bytes):
        token = token.decode()

    return token


if __name__ == "__main__":
    # Generate and print token for manual use
    token = generate_token()
    print("JWT token generated successfully.")
    print("Token (valid for 20 minutes):")
    print(token)
