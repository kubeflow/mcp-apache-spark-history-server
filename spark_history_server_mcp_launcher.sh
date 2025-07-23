#!/bin/bash

# Default values
LOG_PREFIX=""

# Check for -p or --prefix option
if [[ "$1" == "-p" || "$1" == "--prefix" ]] && [[ -n "$2" ]]; then
  LOG_PREFIX="$2"
  shift 2  # Remove these two arguments
fi

# Get the root directory of the git repository
ROOT_DIR="$(git rev-parse --show-toplevel)"

# Set log file name with optional prefix
if [[ -n "$LOG_PREFIX" ]]; then
  LOG_FILE="$ROOT_DIR/${LOG_PREFIX}_mcp_server_output.log"
else
  LOG_FILE="$ROOT_DIR/mcp_server_output.log"
fi

echo "Starting Spark History Server wrapper" > $LOG_FILE
echo "Args: $@" >> $LOG_FILE

# Change to the project directory
cd "$ROOT_DIR" || { echo "Failed to change directory" >> $LOG_FILE; exit 1; }

# Log the current directory
echo "Current directory: $(pwd)" >> $LOG_FILE

echo 'Starting MCP server with new package structure...' >> $LOG_FILE

# Important: Pass through stdin to the agent AND pass the agent's output back to stdout
echo "Starting agent..." >> $LOG_FILE
task start-mcp "$@" 2>> $LOG_FILE
