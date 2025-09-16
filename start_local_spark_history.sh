#!/bin/bash
set -e

# Help function
show_help() {
    cat << EOF
🚀 Starting Local Spark History Server for MCP Testing
=======================================================

USAGE:
    ./start_local_spark_history.sh [OPTIONS]

OPTIONS:
    -h, --help                Show this help message
    --dry-run                 Validate prerequisites without starting the server
    --interactive             Run Docker container in interactive mode
    --spark-version VERSION   Specify Spark version (default: 3.5.5)
    --event-dir PATH          Host path to Spark event directory (default: examples/basic)
    --container-name NAME     Docker container name (default: spark-history-server)
    --port PORT               Port to expose (default: 18080)

DESCRIPTION:
    This script starts a local Spark History Server using Docker for testing
    the Spark History Server MCP. It uses sample Spark event data provided
    in the examples/basic/events/ directory.

PREREQUISITES:
    - Docker must be running
    - Must be run from the project root directory
    - Sample event data must exist in examples/basic/events/

ENDPOINTS:
    - Web UI: http://localhost:18080
    - REST API: http://localhost:18080/api/v1/

EXAMPLES:
    ./start_local_spark_history.sh                       # Start the server with default Spark version (3.5.5)
    ./start_local_spark_history.sh --spark-version=3.5.5 # Start with Spark 3.5.5
    ./start_local_spark_history.sh --help                # Show this help
    ./start_local_spark_history.sh --dry-run             # Validate setup only

EOF
}

# Parse command line arguments
DRY_RUN=false
INTERACTIVE=false
SPARK_VERSION="3.5.5"
EVENT_DIR="examples/basic"
CONTAINER_NAME="spark-history-server"
PORT="18080"

for arg in "$@"; do
    case $arg in
        -h|--help)
            show_help
            exit 0
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --interactive)
            INTERACTIVE=true
            shift
            ;;
        --spark-version=*)
            SPARK_VERSION="${arg#*=}"
            shift
            ;;
        --event-dir=*)
            EVENT_DIR="${arg#*=}"
            shift
            ;;
        --container-name=*)
            CONTAINER_NAME="${arg#*=}"
            shift
            ;;
        --port=*)
            PORT="${arg#*=}"
            shift
            ;;
        *)
            echo "Unknown option: $arg"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

echo "🚀 Starting Local Spark History Server for MCP Testing"
echo "======================================================="

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        echo "❌ Error: Docker is not running. Please start Docker first."
        exit 1
    fi
}

# Function to validate test data
validate_test_data() {
    if [ ! -d "$EVENT_DIR/events" ]; then
        echo "❌ Error: Test data directory '$EVENT_DIR/events' not found."
        echo "   Please ensure the event directory path is correct."
        exit 1
    fi

    if [ ! -f "$EVENT_DIR/history-server.conf" ]; then
        echo "❌ Error: Spark History Server configuration file not found."
        echo "   Expected: $EVENT_DIR/history-server.conf"
        exit 1
    fi
}

# Check prerequisites
echo "🔍 Checking prerequisites..."
check_docker
validate_test_data

# Stop any existing spark-history-server container
echo "🛑 Stopping any existing Spark History Server containers..."
docker stop $CONTAINER_NAME 2>/dev/null && echo "   Stopped existing container" || echo "   No existing container found"
docker rm $CONTAINER_NAME 2>/dev/null && echo "   Removed existing container" || true

echo ""
echo "📊 Available Test Applications:"
echo "==============================="

# Get actual event directories and their sizes
event_dirs=$(ls -1 $EVENT_DIR/events/ 2>/dev/null | grep "eventlog_v2_" | head -10)
if [ -z "$event_dirs" ]; then
    echo "❌ No Spark event logs found in $EVENT_DIR/events/"
    exit 1
fi

# Display available applications with actual sizes
for dir in $event_dirs; do
    app_id=$(echo "$dir" | sed 's/eventlog_v2_//')
    size=$(du -sh "$EVENT_DIR/events/$dir" | cut -f1)
    echo "📋 $app_id ($size)"
done

echo ""
echo "📁 Event directories found:"
ls -1 $EVENT_DIR/events/ | grep eventlog | sed 's/^/   /'

echo ""
echo "📋 Configuration:"
echo "   Log Directory: $(cat $EVENT_DIR/history-server.conf)"
echo "   Port: $PORT"
echo "   Container: $CONTAINER_NAME"
echo "   Docker Image: apache/spark:$SPARK_VERSION"

echo ""
echo "🚀 Starting Spark History Server..."
echo "📍 Will be available at: http://localhost:$PORT"
echo "📍 Web UI: http://localhost:$PORT"
echo "📍 API: http://localhost:$PORT/api/v1/"
echo ""
echo "⚠️  Keep this terminal open - Press Ctrl+C to stop the server"
echo "⚠️  It may take 30-60 seconds for the server to fully start"
echo ""

# Check if this is a dry run
if [ "$DRY_RUN" = true ]; then
    echo "✅ Dry run completed successfully!"
    echo "   All prerequisites are met. Ready to start Spark History Server."
    echo ""
    echo "To start the server, run:"
    echo "   ./start_local_spark_history.sh"
    exit 0
fi

# Start Spark History Server with proper container name and error handling
echo "🐳 Starting Docker container..."
if [ "$INTERACTIVE" = true ]; then
  docker run -it \
    --name $CONTAINER_NAME \
    --label "mcp-spark-test=true" \
    --rm \
    -v "$(pwd)/$EVENT_DIR:/mnt/data" \
    -p $PORT:18080 \
    docker.io/apache/spark:$SPARK_VERSION \
    /opt/java/openjdk/bin/java \
    -cp '/opt/spark/conf:/opt/spark/jars/*' \
    -Xmx1g \
    org.apache.spark.deploy.history.HistoryServer \
    --properties-file /mnt/data/history-server.conf
else
  docker run \
    --name $CONTAINER_NAME \
    --label "mcp-spark-test=true" \
    --rm \
    -v "$(pwd)/$EVENT_DIR:/mnt/data" \
    -p $PORT:18080 \
    docker.io/apache/spark:$SPARK_VERSION \
    /opt/java/openjdk/bin/java \
    -cp '/opt/spark/conf:/opt/spark/jars/*' \
    -Xmx1g \
    org.apache.spark.deploy.history.HistoryServer \
    --properties-file /mnt/data/history-server.conf

  echo "Spark History Server started in detached mode"
  echo "To view logs: docker logs -f $CONTAINER_NAME"
  echo "To stop: docker stop $CONTAINER_NAME"
fi

echo ""
echo "🛑 Spark History Server stopped."
