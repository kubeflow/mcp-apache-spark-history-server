"""Main entry point for Spark History Server MCP."""

import argparse
import json
import logging
import os
import sys

from spark_history_mcp.config.config import load_config, resolve_config_path
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
        default=None,
        help=(
            "Path to config file. If omitted, the server searches "
            "$SHS_MCP_CONFIG, then ./config.yaml, then "
            "~/.config/spark-mcp/config.yaml (env: SHS_MCP_CONFIG)."
        ),
    )
    args = parser.parse_args()

    try:
        logger.info("Starting Spark History Server MCP...")

        # An explicit --config flag wins over a pre-existing SHS_MCP_CONFIG;
        # if omitted, leave the env untouched so discovery can run.
        if args.config is not None:
            os.environ["SHS_MCP_CONFIG"] = args.config

        config_path, _ = resolve_config_path()
        if config_path is not None:
            logger.info(f"Using config file: {config_path}")
        else:
            logger.info(
                "No config file found; using built-in defaults and SHS_* env vars"
            )

        config = load_config()
        if config.mcp.debug:
            logger.setLevel(logging.DEBUG)
        logger.debug(json.dumps(json.loads(config.model_dump_json()), indent=4))
        app.run(config)
    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to start MCP server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
