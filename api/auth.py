import os
import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader


API_KEY_ENV = "API_PRIVATE_KEY"

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(x_api_key: str = Depends(api_key_header)):
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
