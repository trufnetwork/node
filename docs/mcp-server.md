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

```bash
# Install Python 3.12+ from https://www.python.org/downloads/
# or use chocolatey:
choco install python --version=3.12.0

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

## Troubleshooting

**Connection issues:**

- Verify your Kwil node is running
- Check database credentials
- Ensure PostgreSQL port 5432 is accessible from local

**Claude integration:**

- Restart Claude Desktop after installation
- Check config file: `~/Library/Application Support/Claude/claude_desktop_config.json`

## Uninstall

```bash
pipx uninstall postgres-mcp
```
