"""Main entry point for Spark History Server MCP."""

import argparse
import json
import logging
import os
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
    parser = argparse.ArgumentParser(description="Spark History Server MCP")
    parser.add_argument(
        "--config",
        "-c",
        default=os.getenv("SHS_MCP_CONFIG", "config.yaml"),
        help="Path to config file (default: config.yaml, env: SHS_MCP_CONFIG)",
    )
    args = parser.parse_args()

    try:
        logger.info("Starting Spark History Server MCP...")
        logger.info(f"Using config file: {args.config}")
        config = Config.from_file(args.config)
        if config.mcp.debug:
            logger.setLevel(logging.DEBUG)
        logger.debug(json.dumps(json.loads(config.model_dump_json()), indent=4))
        app.run(config)
    except Exception as e:
        logger.error(f"Failed to start MCP server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
