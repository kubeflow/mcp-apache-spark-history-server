# Troubleshooting Guide

## Tools on Large Spark Application Timing Out

Certain tools may timeout based on the size of the Spark application. For large Spark applications that process Gigabytes and Terabytes of data, an increase in timeout and java heap size may need to be applied in order for the tool to have enough time and space to produce an output.

**For example**:
The `compare_job_performance` tool can be slow for large applications.

>`Error calling MCP tool: MCP error -32001: Request timed out`

Apply the following changes to resolve this:

### 1. MCP Client Timeouts

**Problem:**
- MCP Client itself (Q CLI, Kiro, ETC) has its own timeout configuration that can be adjusted.

**Solution:**
Update the `timeout` value in your MCP client configuration (e.g., `mcp.json`):

```json
"spark-history-server": {
  "command": "uv",
  "args": [
    "run",
    "-m",
    "spark_history_mcp.core.main",
    "--frozen"
  ],
  "env": {
    "SHS_MCP_TRANSPORT": "stdio"
  },
  "disabled": false,
  "autoApprove": [],
  "timeout": 300000 <--- Update here
}
```

**Note:** Format depends on client of choice.

### 2: MCP Server Timeouts

**Symptoms:**
- `HTTPConnectionPool(host='localhost', port=18080): Read timed out. (read timeout=30)`
- Server-side timeout errors

**Solution:**
Set the server timeout environment variable:

```json
"spark-history-server": {
  "command": "uv",
  "args": [
    "run",
    "-m",
    "spark_history_mcp.core.main",
    "--frozen"
  ],
  "env": {
    "SHS_MCP_TRANSPORT": "stdio",
    "SHS_SERVERS_LOCAL_TIMEOUT": "500" <-- Set this value in seconds
  },
  "disabled": false,
  "autoApprove": [],
  "timeout": 300000
}
```

**Note:** SHS_SERVERS_<Replace with server name in config.yaml>_TIMEOUT

### 3: JVM Heap Exhaustion

**Symptoms:**
- `HTTP ERROR 500 org.sparkproject.guava.util.concurrent.ExecutionError: java.lang.OutOfMemoryError: Java heap space`
- Browser shows 500 error when accessing Spark History Server URLs
- Tool fails with memory-related errors

**Root Cause:**
Spark History Server parses entire Spark event logs (JSON or Snappy-compressed JSON) into memory. For large jobs (many tasks, long-running, heavy shuffle), the log can be hundreds of MBs to multiple GBs.

**Example Error:**
```
HTTP ERROR 500 org.sparkproject.guava.util.concurrent.ExecutionError: java.lang.OutOfMemoryError: Java heap space
URI:    /history/application_id1/jobs/
STATUS: 500
MESSAGE:    org.sparkproject.guava.util.concurrent.ExecutionError: java.lang.OutOfMemoryError: Java heap space
SERVLET:    org.apache.spark.deploy.history.HistoryServer$$anon$1-5fc930f0
CAUSED BY:  org.sparkproject.guava.util.concurrent.ExecutionError: java.lang.OutOfMemoryError: Java heap space
CAUSED BY:  java.lang.OutOfMemoryError: Java heap space
```

**Solution 1: Enable Hybrid Store (Recommended)**
Configure hybrid store in `spark-defaults.conf` to use disk-backed storage and avoid loading everything into memory:

```properties
# Enable hybrid store to prevent OOM by using disk + memory storage
spark.history.store.hybridStore.enabled true

# Maximum memory usage for in-memory cache (adjust based on available RAM)
spark.history.store.hybridStore.maxMemoryUsage 1g

# Disk backend for overflow storage (ROCKSDB recommended for compatibility)
spark.history.store.hybridStore.diskBackend ROCKSDB

# Serialization format for disk storage (KRYO is more efficient than Java)
spark.history.store.hybridStore.serializer KRYO

# Local directory for disk-backed storage (ensure path exists and is writable)
spark.history.store.path /path/to/local/history-store

# Maximum disk usage to prevent runaway storage growth
spark.history.store.maxDiskUsage 50g
```

**Configuration Details:**
- `hybridStore.enabled`: Enables disk + memory hybrid storage instead of memory-only
- `maxMemoryUsage`: Keeps frequently accessed data in memory for performance
- `diskBackend`: ROCKSDB is more stable than LEVELDB on macOS
- `serializer`: KRYO reduces storage size compared to Java serialization
- `store.path`: Must be a writable local directory (create with `mkdir -p`)
- `maxDiskUsage`: Prevents disk space exhaustion from large applications

**Solution 2: Increase JVM Heap Size**
If hybrid store is not sufficient, increase the Spark daemon memory:

```bash
export SPARK_DAEMON_MEMORY=4g
```

Then restart your Spark History Server.

#### Quick Diagnosis

1. **Check browser first**: Navigate to `http://localhost:18080/history/application_<app_id>/jobs/`
   - If you see HTTP 500 with "OutOfMemoryError" → Problem 3 (increase SPARK_DAEMON_MEMORY)
   - If page loads normally → Problem 1 or 2 (increase timeouts)

2. **Check error message**:
   - `MCP error -32001: Request timed out` → Problem 1 (MCP client timeout)
   - `Read timed out. (read timeout=30)` → Problem 2 (MCP server timeout)
   - `OutOfMemoryError` or `Java heap space` → Problem 3 (JVM heap)
