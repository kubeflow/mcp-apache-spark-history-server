"""Main entry point for Spark History Server MCP."""

import json
import logging
import sys

from spark_history_mcp.config.config import Config
from spark_history_mcp.core import app

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    try:
        logger.info("Starting Spark History Server MCP...")
        config = Config.from_file("config.yaml")
        if config.mcp.debug:
            logger.setLevel(logging.DEBUG)
        logger.debug(json.dumps(json.loads(config.model_dump_json()), indent=4))
        app.run(config)
    except Exception as e:
        logger.error(f"Failed to start MCP server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
