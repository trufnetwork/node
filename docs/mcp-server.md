# TRUF.NETWORK Postgres MCP

Model Context Protocol (MCP) server for querying TRUF.NETWORK data through AI Clients like Claude Desktop.

## Currently Supported AI Client

1. Claude Desktop

Please inquire for support for other clients.

## Prerequisites

1. **Local Kwil Node** - Running TRUF.NETWORK node
2. **Python 3.12+** - Required for MCP server
3. **pipx** - For isolated installation
4. **Claude Desktop** - For AI integration

### Install Prerequisites

**macOS:**

```bash
# Install Python 3.12+
brew install python@3.12

# Install pipx
brew install pipx
pipx ensurepath
```

**Linux (Ubuntu/Debian):**

```bash
# Install Python 3.12+
sudo apt update && sudo apt install python3.12

# Install pipx
python3 -m pip install --user pipx
python3 -m pipx ensurepath
```

**Windows:**

```powershell
# Install Python 3.12+ from https://www.python.org/downloads/
# or use winget:
winget install Python.Python.3.12

# Install pipx
python -m pip install --user pipx
python -m pipx ensurepath
```

**Claude Desktop:**
Download from https://claude.ai/download

## Installation

1. **Clone and install:**

```bash
git clone https://github.com/trufnetwork/postgres-mcp.git
cd postgres-mcp
./install.sh
```

2. **Follow the prompts to configure:**

   - Database connection details (default: localhost:5432/kwild), just use the default if you didn't customize anything.

3. **Restart Claude Desktop**

4. **Easily explore TRUF.NETWORK data with AI! Ask about specific stream IDs, data providers, and more—then let the AI help you run complex calculations or generate tailored suggestions before deciding how to use the data.**

## Available Tools

- `list_schemas` - List database schemas
- `list_objects` - List tables, views, etc.
- `get_object_details` - Get table structure
- `execute_sql` - Run basic SQL queries, including getting records, checking stream type, taxonomies, etc.
- `get_index` - Get stream index data
- `get_index_change` - Calculate percentage changes

## Deployment Options

The TRUF.NETWORK MCP server supports two transport modes:

### 1. Stdio Transport (Default)
Used by Claude Desktop and other local MCP clients that launch processes directly.

### 2. SSE Transport (Server-Sent Events)
For remote connections, multi-client support, and production deployments. Requires additional configuration when deployed behind reverse proxies.

**Starting with SSE Transport:**
```bash
# Using Docker (matches TRUF.NETWORK PostgreSQL setup)
docker run -p 8000:8000 \
  -e DATABASE_URI=postgresql://kwild@localhost:5432/kwild \
  trufnetwork/postgres-mcp \
  --access-mode=restricted \
  --transport=sse \
  --sse-host=0.0.0.0
```

**MCP Client Configuration for SSE:**

**Note**: Claude Desktop doesn't support direct SSE. Use `mcp-remote` proxy:
```json
{
  "mcpServers": {
    "truf-postgres": {
      "command": "npx",
      "args": ["mcp-remote", "http://localhost:8000/sse"]
    }
  }
}
```

**Other MCP clients (Cursor, Cline, Windsurf) with direct SSE support:**
```json
{
  "mcpServers": {
    "truf-postgres": {
      "type": "sse",
      "url": "http://localhost:8000/sse"
    }
  }
}
```

### Reverse Proxy Configuration

When deploying the MCP server behind a reverse proxy (nginx, Caddy, Traefik, etc.), special configuration is required for SSE transport to work correctly.

**📋 See the complete reverse proxy setup guide:** [MCP Server Reverse Proxy Configuration](./mcp-reverse-proxy.md)

The guide covers:
- Nginx, Caddy, Traefik, HAProxy, and Apache configurations
- Essential SSE streaming requirements
- Security considerations
- Testing and troubleshooting
- Production deployment examples

## Troubleshooting

**Connection issues:**

- Verify your Kwil node is running
- Check database credentials
- Ensure PostgreSQL port 5432 is accessible from local

**Claude integration:**

- Restart Claude Desktop after installation
- Check config file locations:
  - **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
  - **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
  - **Linux**: `~/.config/Claude/claude_desktop_config.json`

**Windows-specific issues:**

- **IPv4/IPv6 addressing**: Use `127.0.0.1` instead of `localhost` for SSH tunnels
  ```powershell
  $env:DATABASE_URI = "postgresql://kwild@127.0.0.1:5432/kwild"
  ```

## Uninstall

```bash
pipx uninstall postgres-mcp
```
