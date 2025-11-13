import logging

from dd_internal_authentication.libs.py.dd_internal_authentication.dd_internal_authentication.client import (
    JWTDDToolAuthClientTokenManager,
)

logger = logging.getLogger(__name__)


def get_token(datacenter: str, audience: str) -> str:
    token = JWTDDToolAuthClientTokenManager.instance(
        name=audience, datacenter=datacenter
    ).get_token(audience)
    return str(token)
