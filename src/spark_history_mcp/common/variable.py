import os

POD_NAME = os.getenv("POD_NAME")
POD_NAMESPACE = os.getenv("POD_NAMESPACE", "spark")
POD_SERVICE_ACCOUNT = os.getenv("POD_SERVICE_ACCOUNT", "mcp-spark-history-server")
