#!/usr/bin/env python3
"""
Simple security check for config files to prevent hardcoded credentials.
Detects non-commented username, password, and token fields with actual values.
"""

import logging
import re
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def check_config_file(file_path):
    """Check a YAML config file for hardcoded credentials using regex."""
    errors = []

    try:
        with open(file_path, "r") as f:
            lines = f.readlines()

        for line_num, line in enumerate(lines, 1):
            # Skip commented lines
            stripped_line = line.strip()
            if stripped_line.startswith("#") or not stripped_line:
                continue

            # Look for credential fields with values
            # Pattern: field_name: "value" or field_name: value
            credential_pattern = r'^\s*(username|password|token)\s*:\s*["\']?([^"\'\s#]+)["\']?\s*(?:#.*)?$'
            match = re.match(credential_pattern, line, re.IGNORECASE)

            if match:
                field_name = match.group(1)
                field_value = match.group(2)

                # Skip environment variable references
                if field_value.startswith("${") and field_value.endswith("}"):
                    continue

                # Skip obvious placeholders
                placeholders = [
                    "your_",
                    "example_",
                    "placeholder",
                    "<",
                    ">",
                    "changeme",
                    "replace",
                    "todo",
                    "fixme",
                    "xxx",
                    "yyy",
                    "zzz",
                ]
                if any(
                    placeholder in field_value.lower() for placeholder in placeholders
                ):
                    continue

                # If we get here, it's likely a real credential
                errors.append(
                    f"Line {line_num}: Hardcoded {field_name} found: '{field_value}'"
                )

    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        errors.append(f"Error reading {file_path}: {e}")

    return errors


def main():
    """Main function to check config files."""
    config_files = [
        "config.yaml",
    ]

    all_errors = []

    for config_file in config_files:
        if Path(config_file).exists():
            errors = check_config_file(config_file)
            if errors:
                all_errors.extend([f"{config_file}: {error}" for error in errors])

    if all_errors:
        logging.error("🚨 SECURITY ISSUE: Hardcoded credentials detected!")
        logging.error("❌ The following files contain hardcoded credentials:")
        for error in all_errors:
            logging.error(f"  • {error}")
        logging.info("✅ Fix by using environment variables instead:")
        logging.info('  username: "${SHS_SPARK_USERNAME}"')
        logging.info('  password: "${SHS_SPARK_PASSWORD}"')
        logging.info('  token: "${SHS_SPARK_TOKEN}"')
        return 1

    logging.info("✅ No hardcoded credentials found in config files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
