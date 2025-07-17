# 📸 Screenshots

This directory contains screenshots for the README.md file.

## 📋 Required Screenshots

### 🔍 Get Applications Screenshot
**Filename**: `get-applications.png`

**What to capture**:
1. Open MCP Inspector in browser (http://localhost:6274)
2. Click on `list_applications` tool in the sidebar
3. Click "Call Tool" button
4. Capture the results showing all 3 Spark applications:
   - spark-bcec39f6201b42b9925124595baad260
   - spark-110be3a8424d4a2789cb88134418217b
   - spark-cc4d115f011443d787f03a71a476a745

**Screenshot should show**:
- Tool name and parameters (if any)
- JSON response with application list
- Application IDs, names, and status

### ⚡ Job Comparison Screenshot
**Filename**: `job-comparison.png`

**What to capture**:
1. In MCP Inspector, click on `compare_job_performance` tool
2. Fill in parameters:
   - `app_id1`: `spark-bcec39f6201b42b9925124595baad260`
   - `app_id2`: `spark-110be3a8424d4a2789cb88134418217b`
3. Click "Call Tool" button
4. Capture the comparison results

**Screenshot should show**:
- Tool name and input parameters
- Performance comparison results
- Metrics like duration, stages, tasks, etc.
- Any performance differences highlighted

## 🎯 Screenshot Guidelines

- **Resolution**: Capture at high resolution (retina/2x if possible)
- **Browser**: Use clean browser window, hide bookmarks bar
- **Focus**: Ensure the tool execution and results are clearly visible
- **Format**: Save as PNG for best quality
- **Content**: Make sure all important data is visible and readable

## 📝 Usage in README

These screenshots are referenced in the main README.md file:

```markdown
### 🔍 Get Spark Applications
![Get Applications](screenshots/get-applications.png)
*Browse all available Spark applications with filtering options*

### ⚡ Job Performance Comparison
![Job Comparison](screenshots/job-comparison.png)
*Compare performance metrics between different Spark jobs*
```

Replace the placeholder screenshots with actual MCP Inspector captures to show users what to expect when testing the tools.
