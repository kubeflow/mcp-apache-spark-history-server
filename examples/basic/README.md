# Spark History Server Test Setup

## Real Spark Event Data

This directory contains real Spark application event logs for testing the MCP server functionality. All applications completed successfully and are ideal for testing job comparison features.

### Test Applications

- **`eventlog_v2_spark-bcec39f6201b42b9925124595baad260/`**
  - **Status**: ✅ Completed Successfully
  - **Use Case**: Basic functionality testing and job comparison baseline

- **`eventlog_v2_spark-110be3a8424d4a2789cb88134418217b/`**
  - **Status**: ✅ Completed Successfully
  - **Use Case**: Job comparison testing (compare with bcec39f application)

- **`eventlog_v2_spark-cc4d115f011443d787f03a71a476a745/`**
  - **Status**: ✅ Completed Successfully
  - **Use Case**: Performance analysis and multi-application comparison

### Starting the History Server

Run the Spark History Server with the real event data:

```bash
podman run -it -v $(pwd)/examples/basic:/mnt/data -p 18080:18080 spark:3.5.5 \
  /opt/java/openjdk/bin/java -cp '/opt/spark/conf:/opt/spark/jars/*' -Xmx1g \
  org.apache.spark.deploy.history.HistoryServer \
  --properties-file /mnt/data/history-server.conf
```

### Testing Scenarios

1. **Basic Functionality**: Use any application for quick MCP tool validation
   ```bash
   # Test with: spark-bcec39f6201b42b9925124595baad260
   ```

2. **Job Comparison**: Compare configurations and performance between applications
   ```bash
   # Compare: spark-bcec39f6201b42b9925124595baad260 vs spark-110be3a8424d4a2789cb88134418217b
   ```

3. **Multi-Application Analysis**: Test with all 3 applications for comprehensive analysis
   ```bash
   # All apps: bcec39f, 110be3a, cc4d115
   ```

### Accessing the History Server

- Web UI: http://localhost:18080
- REST API: http://localhost:18080/api/v1/applications
- MCP Server: Connect to your MCP server pointing to http://localhost:18080
