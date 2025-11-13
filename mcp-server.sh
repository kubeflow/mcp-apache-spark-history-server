export VAULT_ADDR=https://vault.us1.staging.dog
vault login -method=oidc
export VAULT_TOKEN=$(vault print token)

aws-vault exec sso-staging-engineering -- uv run -m spark_history_mcp.core.main