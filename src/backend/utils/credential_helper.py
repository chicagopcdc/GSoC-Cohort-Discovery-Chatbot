"""Gen3 authentication token management.

Generates access tokens for the PCDC Gen3 Guppy / GraphQL API using a
local credentials.json refresh-token file. Called by app.py before
outgoing GraphQL requests that require authentication.
"""

import os
from gen3.auth import Gen3Auth
from fastapi import HTTPException
import logging
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_access_token() -> str:
    """Obtain a short-lived access token from the Gen3 auth service.

    Reads the refresh token from credentials.json and exchanges it
    for a fresh access token via the Gen3 SDK.

    Raises:
        HTTPException: With status 500 if the credentials file is
            missing or the token exchange fails.
    """
    base_url = "https://portal-dev.pedscommons.org"
    credentials_file = "./credentials.json" 
    try:
        if not os.path.exists(credentials_file):
            raise FileNotFoundError(f"Credentials file not found at {credentials_file}")
        auth = Gen3Auth(endpoint=base_url, refresh_file=credentials_file)
        access_token = auth.get_access_token()
        logger.info("Successfully generated new access token")
        return access_token
    except Exception as e:
        logger.error(f"Failed to get access token: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to authenticate: {str(e)}"
        )