# Spark History Server MCP for Amazon EMR

📺 **See it in action:**

[![Watch the demo video](https://img.shields.io/badge/YouTube-Watch%20Demo-red?style=for-the-badge&logo=youtube)](https://www.youtube.com/watch?v=FaduuvMdGxI)

If you are an existing Amazon EMR user looking to analyze your Spark Applications, you have **two options**:

1. **🔧 Static Configuration** - Pre-configure EMR clusters in `config.yaml`
2. **⚡ Dynamic Configuration** - Specify EMR clusters directly in tool calls

The dynamic approach is particularly useful when analyzing Spark applications across multiple EMR clusters without pre-configuration.

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

## Step 2: Use Amazon EMR Persistent UI

Amazon EMR-EC2 users can use a service-managed [Persistent UI](https://docs.aws.amazon.com/emr/latest/ManagementGuide/app-history-spark-UI.html) which automatically creates the Spark History Server for Spark applications on a given EMR Cluster. You can directly go to Step 3 and configure the MCP server with an EMR Cluster Id to analyze the Spark applications on that cluster.

## Step 3: Configure the MCP Server

You have **two configuration options**:

### Option A: 🔧 Static Configuration

- Identify the Amazon EMR Cluster Id for which you want the MCP server to analyze the Spark applications
- Edit SHS MCP Config: [config.yaml](../../../config.yaml) to add the EMR Cluster Id

```yaml
servers:
  emr_persistent_ui:
    emr_cluster_arn: "<emr_cluster_arn>"

dynamic_emr_clusters_mode: false  # Disable dynamic mode
```

### Option B: ⚡ Dynamic Configuration

Enable dynamic EMR clusters mode in [config.yaml](../../../config.yaml):

```yaml
dynamic_emr_clusters_mode: true  # Enable dynamic mode

# No need to pre-configure servers - specify clusters in tool calls
```

With dynamic mode, you can specify EMR clusters directly in AI queries:
- **By ARN**: `"arn:aws:emr:us-east-1:123456789012:cluster/j-1234567890ABC"`
- **By Cluster ID**: `"j-1234567890ABC"`
- **By Cluster Name**: `"my-production-cluster"` (active clusters only)

**Note**: The MCP Server manages the creation of the Persistent UI and its authentication using tokens with Persistent UI. You do not need to open the Persistent UI URL in a Web Browser. Please ensure the user running the MCP has access to create and view the Persistent UI for that cluster by following the [EMR Documentation](https://docs.aws.amazon.com/emr/latest/ManagementGuide/app-history-spark-UI.html#app-history-spark-UI-permissions).

## Step 4: Start the MCP Server

```bash
task start-mcp-bg
```

If you need to access an EMR Spark History Server running in private subnets:

```bash
# Set up SSH tunnel through bastion host, or use another tunneling mechanism of your preference
ssh -AL 8157:localhost:8157 ec2-user@YOUR_BASTION_DNS -t ssh -ND 8157 hadoop@YOUR_PRIMARY_NODE_PRIVATE_DNS

# Use the tool with proxy enabled
SHS_SERVERS_LOCAL_URL=http://YOUR_PRIMARY_NODE_PRIVATE_DNS:18080 USE_PROXY=1 task start-mcp
```

**Note**: When configuring an EMR cluster ARN, the MCP server will automatically check for an existing Persistent UI. If one does not exist, it will create a new Persistent UI for the specified cluster. If a Persistent UI already exists, the server will use the existing one. This creation happens automatically during server initialization to enable Spark History Server access.

## Step 5: Interact with the MCP Server using an AI Agent

You can use an AI Agent to start interacting with the Spark History MCP server following the steps for [Amazon Q CLI](../../../examples/integrations/amazon-q-cli/README.md) or [Claude Desktop](../../../examples/integrations/claude-desktop/README.md). For more instructions on other Agents, please refer to the AI Agent Integration section in the main README.
