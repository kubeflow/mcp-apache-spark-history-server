import logging
import os

import httpx
from httpx_retries import Retry, RetryTransport

from spark_history_mcp.common.variable import POD_NAME

logger = logging.getLogger(__name__)


class JWT:
    def __init__(self, audience: str, datacenter: str):
        self.audience = audience

        from dd_internal_authentication.libs.py.dd_internal_authentication.dd_internal_authentication.client import (
            JWTDDToolAuthClientTokenManager,
            JWTInternalServiceAuthClientTokenManager,
        )

        if POD_NAME:
            logger.info("Using internal service auth client")
            self.token_manager = JWTInternalServiceAuthClientTokenManager(
                issuer="sycamore"
            )
        else:
            logger.info("Using internal ddtool auth client")
            self.token_manager = JWTDDToolAuthClientTokenManager.instance(
                name=self.audience, datacenter=datacenter
            )

    def get_token(self) -> str:
        return str(self.token_manager.get_token(self.audience))


class VaultApi:
    VAULT_URL = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8658/vault/agent")
    DEFAULT_TIMEOUT_SECONDS = 30

    def __init__(self):
        self.transport = RetryTransport(retry=Retry(total=5, backoff_factor=0.5))

    def get_secret_kv_store(self, secret_path: str, key: str) -> str | None:
        """
        Fetch a secret from the kv Vault store:
        docs: https://datadoghq.atlassian.net/wiki/spaces/RUNTIME/pages/2701559033/Vault#Application-(v1)-versus-KV-(v2)-stores
        :param secret_path: a path to a secret:
        :param key: key to fetch from the secret:
        :return: the secret data for the specific key
        """
        logger.info(
            "Fetching secret from Vault",
            extra={"kv_backend": "kv", "path": secret_path},
        )

        headers = {"X-Vault-Request": "true"}
        if POD_NAME is None:
            headers["X-Vault-Token"] =os.getenv("VAULT_TOKEN")
        with httpx.Client(transport=self.transport) as client:
            response = client.get(
                f"{self.VAULT_URL}/v1/kv/data/{secret_path}",
                headers=headers,
                timeout=self.DEFAULT_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            # kv-v2 has a nested structure, see
            # https://www.vaultproject.io/api/secret/kv/kv-v2#read-secret-version for an example
            secret_data = response.json().get("data", {}).get("data", None)

            if not secret_data:
                logger.warning(
                    f"Could not find secret data in kv-v2 store for path: {secret_path}"
                )
                return None

            return secret_data.get(key, None)
