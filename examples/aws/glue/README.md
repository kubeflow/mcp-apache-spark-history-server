# Spark History Server MCP for AWS Glue

📺 **See it in action:**

[![Watch the demo video](https://img.shields.io/badge/YouTube-Watch%20Demo-red?style=for-the-badge&logo=youtube)](https://www.youtube.com/watch?v=FaduuvMdGxI)

If you are an existing AWS Glue user looking to analyze your Spark Applications, then you can follow the steps below to start using the Spark History Server MCP in 5 simple steps.

## Step 1: Setup project on your laptop

Follow the Quick Setup instructions to git clone the Spark History Server MCP project on your laptop:

```bash
git clone https://github.com/DeepDiagnostix-AI/mcp-apache-spark-history-server.git
cd mcp-apache-spark-history-server

# Install Task (if not already installed)
brew install go-task  # macOS, see https://taskfile.dev/installation/ for others
# Setup and start testing
task install                    # Install dependencies
```

## Step 2: Install a new Spark History Server or use an existing Spark History Server

You can follow the AWS Glue [public documentation](https://docs.aws.amazon.com/glue/latest/dg/monitor-spark-ui-history.html) to setup a new self-managed Spark History Server for your AWS Glue Jobs. If you already have a Spark History Server setup, then simply use it and identify its Spark UI URL and port for Step 3.

## Step 3: Configure the MCP Server to use the Spark History Server

Edit the MCP Server config to specify SparkUI URL/Port:

**Option 1**: Spark History Server on EC2

- Identify the SparkUiPrivateUrl or SparkUiPublicUrl (based on your subnet being private or public) from Step 2 and ensure you can open it in a web browser
- Edit SHS MCP Config: [config.yaml](../../../config.yaml) and add the Spark UI URL and port

```yaml
glue_ec2:
  url: "<SparkUiUrl>:<port>"
  verify_ssl: false
```

**Note**: Since the URL is self-signed, the MCP server does not need to verify the SSL connection.

**Option 2**: Spark History Server on Local Docker Container

- Identify and open the Spark UI in your web browser at: http://localhost:18080
- Edit SHS MCP Config: [config.yaml](../../../config.yaml) to specify the local server information

```yaml
local:
    default: true
    url: "http://localhost:18080"
```

## Step 4: Start the MCP Server

```bash
task start-mcp-bg
```

## Step 5: Interact with the MCP Server using an AI Agent

You can use an AI Agent to start interacting with the Spark History MCP server following the steps for [Amazon Q CLI](../../../examples/integrations/amazon-q-cli/README.md) or [Claude Desktop](../../../examples/integrations/claude-desktop/README.md). For more instructions on other Agents, please refer to the AI Agent Integration section in the main README.
