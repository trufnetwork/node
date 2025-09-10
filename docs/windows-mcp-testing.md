# Windows MCP Server + Reverse Proxy Testing Guide

This guide will help you test the TRUF.NETWORK MCP server **natively on Windows** with reverse proxy configuration, connecting to your AWS database.

## Prerequisites ✅

You already have:
- ✅ TRUF.NETWORK node running on AWS
- ✅ Windows machine 
- ✅ Claude Desktop installed on Windows
- ✅ Database with TRUF.NETWORK data populated

## Required Software

### Install on Windows (Native):
1. **Python 3.12+** - Download from https://www.python.org/downloads/
2. **pipx** - For isolated Python package installation
3. **Git** - For cloning the repository
4. **Caddy** - For reverse proxy testing (https://caddyserver.com/download)

### Install Commands (Windows PowerShell as Administrator):

```powershell
# Install Python 3.12+ (or download from python.org)
winget install Python.Python.3.12

# Install pipx
python -m pip install --user pipx
python -m pipx ensurepath

# Install Git (if not already installed)
winget install Git.Git

# Download Caddy from https://caddyserver.com/download
```

---

## Step 1: Set Up SSH Tunnel (if the node is being set up remotely from local machine)

### **Windows PowerShell 1: SSH Port Forward** 
**Keep this running throughout the entire test!**

```powershell
ssh -i ~/.ssh/<your-key.pem> -L 5432:localhost:5432 <username>@<aws-node-ip>
```

**✅ Test the tunnel works:**
```powershell
# In another PowerShell window
Test-NetConnection -ComputerName localhost -Port 5432
# Should return: TcpTestSucceeded: True
```

---

## Step 2: Install MCP Server Natively on Windows

### **Windows PowerShell 2: Clone and Install**

```powershell
# Navigate to your development directory
cd C:\Users\$env:USERNAME

# Clone the postgres-mcp repository 
git clone https://github.com/trufnetwork/postgres-mcp.git
cd postgres-mcp

# Run the install script
./install.sh
```

**During installation, you'll be prompted for:**
- Host: `localhost` (default)
- Port: `5432` (default)  
- Database: `kwild` (default)
- Username: `kwild` (default)
- Password: (leave empty - uses trust authentication)

**✅ Expected Results:**
- MCP server installed with pipx
- Claude Desktop config created/updated at `%APPDATA%\Claude\claude_desktop_config.json`
- Database connection tested successfully

---

## Step 3: Test Basic MCP Functionality (Stdio Transport)

### **Windows PowerShell 3: Test MCP Server**

```powershell
# Test the installed postgres-mcp command
$env:DATABASE_URI = "postgresql://kwild@localhost:5432/kwild"
postgres-mcp --help
```

### Test Claude Desktop Integration

1. **Restart Claude Desktop completely**
2. **Open a new conversation**
3. **Test basic functionality:**

```
Can you list the available MCP tools?
```

```
Can you show me what schemas are available in the database?
```

**✅ Expected Results:**
- Claude shows MCP tools available
- Can query main schema and TRUF.NETWORK tables
- Database queries return real data

---

## Step 4: Set Up Reverse Proxy Testing (SSE Transport)

### **Important IPv4 Configuration Note**
⚠️ **Windows networking may use IPv6 (::1) by default, but SSH tunnels typically work on IPv4 (127.0.0.1). Use IPv4 addresses explicitly to avoid connection timeouts.**

### **Windows PowerShell 4: Start MCP Server with SSE**

```powershell
# Start MCP server with SSE transport using IPv4 addressing (keep this running!)
$env:DATABASE_URI = "postgresql://kwild@127.0.0.1:5432/kwild"
postgres-mcp --access-mode=restricted --transport=sse --sse-host=127.0.0.1 --sse-port=8000
```

**✅ Expected Output:**
```
Starting PostgreSQL MCP Server in RESTRICTED mode
Successfully connected to database and initialized connection pool
Starting SSE server on 127.0.0.1:8000
```

**❌ If you see database timeouts:**
- Ensure SSH tunnel is running
- Use IPv4 (127.0.0.1) instead of localhost
- Verify with `Test-NetConnection -ComputerName localhost -Port 5432`

### **Windows PowerShell 5: Set Up Caddy Reverse Proxy**

Create Caddyfile:
```powershell
# Create Caddyfile in current directory
@"
:8080 {
    reverse_proxy /sse* 127.0.0.1:8000 {
        flush_interval -1
        header_up Host {http.reverse_proxy.upstream.hostport}
        header_up Connection {>Connection}
        header_up Cache-Control {>Cache-Control}
    }
}
"@ | Out-File -FilePath "Caddyfile" -Encoding UTF8
```

Start Caddy:
```powershell
# Start Caddy (keep this running!)
caddy run
```

**✅ Expected Output:**
- Caddy starts without errors
- Serving HTTP on :8080

---

## Step 5: Test Reverse Proxy Endpoints

### **Windows PowerShell 6: Test Direct and Proxied Connections**

**⚠️ PowerShell Curl Note**: Use `curl.exe` or `Invoke-WebRequest` to avoid PowerShell's curl alias issues.

```powershell
# Test direct connection to MCP server
curl.exe -H "Accept: text/event-stream" http://127.0.0.1:8000/sse

# Test through Caddy reverse proxy
curl.exe -H "Accept: text/event-stream" http://127.0.0.1:8080/sse

# Alternative using PowerShell native command
Invoke-WebRequest -Uri "http://127.0.0.1:8000/sse" -Headers @{"Accept"="text/event-stream"}
Invoke-WebRequest -Uri "http://127.0.0.1:8080/sse" -Headers @{"Accept"="text/event-stream"}
```

**✅ Expected Results:**
- Both endpoints should respond with SSE headers
- No buffering or delays
- Similar response format

---

## Step 6: Configure Claude Desktop for SSE Transport

### **Edit Claude Desktop Config for SSE Testing**

Edit: `%APPDATA%\Claude\claude_desktop_config.json`

**⚠️ Important**: Claude Desktop doesn't support direct SSE connections. You must use `mcp-remote` proxy tool.

**Replace with this configuration to test the reverse proxy:**

```json
{
  "mcpServers": {
    "truf-proxy": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "http://127.0.0.1:8080/sse"
      ]
    }
  }
}
```

**🔄 Optional: Test Direct Connection**
To compare performance, you can also test the direct connection separately:
```json
{
  "mcpServers": {
    "truf-direct": {
      "command": "npx",
      "args": [
        "mcp-remote", 
        "http://127.0.0.1:8000/sse"
      ]
    }
  }
}
```

**⚠️ Note:** Don't enable both direct and proxy connections simultaneously as this can cause conflicts. Test them one at a time.

**Save the file and completely restart Claude Desktop!**

---

## Step 7: Test SSE Transport in Claude Desktop

### **Test Both Configurations**

1. **Close Claude Desktop completely**
2. **Start Claude Desktop again**
3. **Open a new conversation**
4. **Test connection:**

```
Can you list the available MCP tools?
```

**✅ Expected Results:**
- Claude should show MCP tools from the configured server
- Database queries work through the reverse proxy
- Performance should be similar to direct connection

---

## Terminal Layout Summary

**Windows PowerShell Windows:**
1. **PowerShell 1**: SSH tunnel to AWS (keep running)
2. **PowerShell 2**: Installation and testing (can close after setup)
3. **PowerShell 3**: Basic stdio testing (can close after testing)
4. **PowerShell 4**: MCP server with SSE transport (keep running)
5. **PowerShell 5**: Caddy reverse proxy (keep running)
6. **PowerShell 6**: Testing endpoints (can close after testing)

**Windows Applications:**
- **Claude Desktop**: Configured for SSE transport testing

---

## Troubleshooting

### **Common Issues**

**1. Python/pipx not found:**
```powershell
# Add Python to PATH
$env:PATH += ";C:\Users\$env:USERNAME\AppData\Local\Programs\Python\Python312"
refreshenv
```

**2. postgres-mcp command not found:**
```powershell
# Add pipx binaries to PATH
$env:PATH += ";C:\Users\$env:USERNAME\AppData\Roaming\Python\Python312\Scripts"
refreshenv
```

**3. SSH tunnel disconnected:**
```powershell
# Check if tunnel is still active
netstat -an | findstr :5432
```

**4. Database connection timeouts:**
```powershell
# Force IPv4 addressing - most common fix
$env:DATABASE_URI = "postgresql://kwild@127.0.0.1:5432/kwild"

# Check connectivity
Test-NetConnection -ComputerName localhost -Port 5432

# Should return: TcpTestSucceeded: True
```

**5. PowerShell curl header errors:**
```powershell
# Wrong (PowerShell alias issue):
curl -H "Accept: text/event-stream" http://localhost:8000/sse

# Correct options:
curl.exe -H "Accept: text/event-stream" http://127.0.0.1:8000/sse
Invoke-WebRequest -Uri "http://127.0.0.1:8000/sse" -Headers @{"Accept"="text/event-stream"}
```

**6. Caddy port conflict:**
```powershell
# Check what's using port 8080
netstat -ano | findstr :8080
```

**7. Claude Desktop SSE connection issues:**
- Verify both MCP server (port 8000) and Caddy (port 8080) are running
- Check Windows Firewall isn't blocking connections
- Test direct connection first to ensure MCP server works
- Don't enable both direct and proxy connections simultaneously
- Use `npx mcp-remote` instead of direct SSE configuration

**8. Claude Desktop "command" field Required errors:**
- Claude Desktop requires the `command` field for all MCP server configurations
- Direct SSE configuration is not supported - must use `mcp-remote` proxy

### **Debug Commands**

```powershell
# Check running processes
Get-Process | Where-Object {$_.ProcessName -match "postgres-mcp|caddy"}

# Test port connectivity
Test-NetConnection -ComputerName 127.0.0.1 -Port 8000
Test-NetConnection -ComputerName 127.0.0.1 -Port 8080

# Check services listening
netstat -an | findstr :8000
netstat -an | findstr :8080
```

---

## Expected Results ✅

**✅ Success indicators:**
- MCP server installs natively on Windows with pipx
- Basic stdio transport works with Claude Desktop  
- SSE transport starts on port 8000 with IPv4 addressing
- Caddy reverse proxy runs on port 8080
- Proxied endpoint (8080) responds correctly
- Claude Desktop connects via SSE transport through mcp-remote proxy
- Database queries work through the reverse proxy connection
- No performance degradation through reverse proxy
- Streaming behavior works properly (no buffering)

**❌ Failure indicators:**
- Installation errors with pipx or Python
- Claude Desktop shows no MCP tools
- SSE endpoints return errors or timeouts
- Database connection timeout errors (usually IPv6/IPv4 issue)
- Reverse proxy buffering causes delays
- PowerShell curl header syntax errors

---

## Key Differences from Linux/Docker Deployment

### Windows-Specific Considerations

1. **IPv4/IPv6 Addressing**: Windows may default to IPv6 (`::1`) while SSH tunnels work on IPv4 (`127.0.0.1`). Always use explicit IPv4 addresses.

2. **PowerShell curl Alias**: Use `curl.exe` or `Invoke-WebRequest` instead of PowerShell's `curl` alias to avoid header parameter issues.

3. **PATH Management**: Python and pipx binaries may need manual PATH configuration.

4. **Claude Desktop Config Location**: `%APPDATA%\Claude\claude_desktop_config.json` on Windows vs `~/Library/Application Support/Claude/claude_desktop_config.json` on macOS.

5. **mcp-remote Requirement**: Claude Desktop requires `npx mcp-remote` proxy for SSE connections - direct SSE configuration is not supported.

### Production Deployment Considerations

Once Windows testing works:

1. **Deploy to production server** with proper domain names
2. **Add HTTPS certificates** 
3. **Test with multiple Claude Desktop clients** simultaneously
4. **Benchmark performance** under load
5. **Document production deployment** procedures

This testing validates the complete TRUF.NETWORK MCP server reverse proxy functionality on Windows!