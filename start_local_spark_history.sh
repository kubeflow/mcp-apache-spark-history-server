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
            spark_version="${arg#*=}"
            shift
            ;;
        --spark-version)
            shift
            if [ -n "$1" ] && [ "${1:0:1}" != "-" ]; then
                spark_version="$1"
                shift
            else
                echo "Error: Argument for $arg is missing" >&2
                exit 1
            fi
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
    if [ ! -d "examples/basic/events" ]; then
        echo "❌ Error: Test data directory 'examples/basic/events' not found."
        echo "   Please ensure you're running this script from the project root directory."
        exit 1
    fi

    if [ ! -f "examples/basic/history-server.conf" ]; then
        echo "❌ Error: Spark History Server configuration file not found."
        echo "   Expected: examples/basic/history-server.conf"
        exit 1
    fi
}

# Check prerequisites
echo "🔍 Checking prerequisites..."
check_docker
validate_test_data

# Stop any existing spark-history-server container
echo "🛑 Stopping any existing Spark History Server containers..."
docker stop spark-history-server 2>/dev/null && echo "   Stopped existing container" || echo "   No existing container found"
docker rm spark-history-server 2>/dev/null && echo "   Removed existing container" || true

echo ""
echo "📊 Available Test Applications:"
echo "==============================="

# Get actual event directories and their sizes
event_dirs=$(ls -1 examples/basic/events/ 2>/dev/null | grep "eventlog_v2_" | head -10)
if [ -z "$event_dirs" ]; then
    echo "❌ No Spark event logs found in examples/basic/events/"
    exit 1
fi

# Display available applications with actual sizes
for dir in $event_dirs; do
    app_id=$(echo "$dir" | sed 's/eventlog_v2_//')
    size=$(du -sh "examples/basic/events/$dir" | cut -f1)
    echo "📋 $app_id ($size)"
done

echo ""
echo "📁 Event directories found:"
ls -1 examples/basic/events/ | grep eventlog | sed 's/^/   /'

echo ""
echo "📋 Configuration:"
echo "   Log Directory: $(cat examples/basic/history-server.conf)"
echo "   Port: 18080"
echo "   Docker Image: apache/spark:$spark_version"

echo ""
echo "🚀 Starting Spark History Server..."
echo "📍 Will be available at: http://localhost:18080"
echo "📍 Web UI: http://localhost:18080"
echo "📍 API: http://localhost:18080/api/v1/"
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
    --name spark-history-server \
    --rm \
    -v "$(pwd)/examples/basic:/mnt/data" \
    -p 18080:18080 \
    docker.io/apache/spark:$spark_version \
    /opt/java/openjdk/bin/java \
    -cp '/opt/spark/conf:/opt/spark/jars/*' \
    -Xmx1g \
    org.apache.spark.deploy.history.HistoryServer \
    --properties-file /mnt/data/history-server.conf
else
  docker run \
    --name spark-history-server \
    --rm \
    -v "$(pwd)/examples/basic:/mnt/data" \
    -p 18080:18080 \
    docker.io/apache/spark:$spark_version \
    /opt/java/openjdk/bin/java \
    -cp '/opt/spark/conf:/opt/spark/jars/*' \
    -Xmx1g \
    org.apache.spark.deploy.history.HistoryServer \
    --properties-file /mnt/data/history-server.conf

  echo "Spark History Server started in detached mode"
  echo "To view logs: docker logs -f spark-history-server"
  echo "To stop: docker stop spark-history-server"
fi

echo ""
echo "🛑 Spark History Server stopped."
