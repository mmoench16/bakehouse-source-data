import os
import secrets

from fastapi import Header, HTTPException, status


API_KEY_ENV = "API_PRIVATE_KEY"


def verify_api_key(x_api_key: str = Header(default="", alias="X-API-Key")):
    expected_api_key = os.getenv(API_KEY_ENV)
    if not expected_api_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Missing server configuration: {API_KEY_ENV}",
        )

    if not x_api_key or not secrets.compare_digest(x_api_key, expected_api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
        )
