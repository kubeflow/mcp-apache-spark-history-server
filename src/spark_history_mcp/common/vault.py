import os


class JWT:
    def __init__(self, audience: str, datacenter: str):
        self.audience = audience

        from dd_internal_authentication.libs.py.dd_internal_authentication.dd_internal_authentication.client import (
            JWTDDToolAuthClientTokenManager,
            JWTInternalServiceAuthClientTokenManager,
        )

        if os.getenv("POD_NAME"):
            token_manager_class = JWTInternalServiceAuthClientTokenManager
        else:
            token_manager_class = JWTDDToolAuthClientTokenManager

        self.token_manager = token_manager_class.instance(
            name=self.audience, datacenter=datacenter
        )

    def get_token(self) -> str:
        try:
            return str(self.token_manager.get_token(self.audience))
        except Exception as e:
            raise RuntimeError(f"Failed to get authentication token: {str(e)}")
