"""Main entry point for Spark History Server MCP."""

import logging
import sys

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
        app.run()
    except Exception as e:
        logger.error(f"Failed to start MCP server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
