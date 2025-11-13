export VAULT_ADDR=https://vault.us1.staging.dog
vault login -method=oidc
export VAULT_TOKEN=$(vault print token)

uv run -m spark_history_mcp.core.main