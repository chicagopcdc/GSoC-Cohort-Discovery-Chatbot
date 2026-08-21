"""
Gen3 access token for the endpoints that actually need one.

Aggregate counts do not: https://portal.pedscommons.org/guppy/graphql/ answers
anonymously, and that is how cohort counting works here. This is only for
line-level or otherwise restricted access, so nothing on the counting path
should import it.

Both the commons URL and the credentials file come from the environment.
`gen3` is imported inside the function so that a deployment without the package
loses this one call rather than failing to start.
"""

import logging
import os

from fastapi import HTTPException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://portal.pedscommons.org"
DEFAULT_CREDENTIALS_FILE = "./credentials.json"


def generate_access_token() -> str:
    base_url = os.getenv("GEN3_BASE_URL", DEFAULT_BASE_URL)
    credentials_file = os.getenv("GEN3_CREDENTIALS_FILE", DEFAULT_CREDENTIALS_FILE)

    try:
        from gen3.auth import Gen3Auth
    except ImportError as e:
        raise HTTPException(
            status_code=500,
            detail=(
                "gen3 is not installed, so no access token can be issued. "
                "Aggregate counts do not need one; only restricted access does."
            ),
        ) from e

    try:
        if not os.path.exists(credentials_file):
            raise FileNotFoundError(
                f"Credentials file not found at {credentials_file}. "
                "Set GEN3_CREDENTIALS_FILE to point at your own key."
            )
        auth = Gen3Auth(endpoint=base_url, refresh_file=credentials_file)
        access_token = auth.get_access_token()
        logger.info("Successfully generated new access token")
        return access_token
    except Exception as e:
        logger.error(f"Failed to get access token: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to authenticate: {str(e)}",
        )
