# MCP Server Reverse Proxy Configuration

This guide explains how to configure reverse proxy servers for TRUF.NETWORK's MCP (Model Context Protocol) server when using SSE (Server-Sent Events) transport.

💡 **Quick Start**: Ready-to-use configuration files and Docker Compose setups are available in [`examples/mcp-reverse-proxy/`](./examples/mcp-reverse-proxy/).

## Overview

The TRUF.NETWORK MCP server supports two transport modes:
- **stdio**: Direct process communication (default, used by Claude Desktop)
- **sse**: Server-Sent Events over HTTP (for remote connections and multi-client support)

When deploying the MCP server with SSE transport behind a reverse proxy, specific configuration is required to ensure proper streaming behavior.

## Why Reverse Proxy Configuration Matters

SSE (Server-Sent Events) is a streaming technology that maintains persistent HTTP connections to deliver real-time data. Unlike regular HTTP requests, SSE requires:

1. **No buffering** - Data must be streamed immediately, not buffered
2. **Persistent connections** - Long-lived connections that don't timeout prematurely
3. **Proper headers** - Specific HTTP headers for event streaming
4. **HTTP/1.1 support** - Streaming requires HTTP/1.1 features

## Critical Configuration Requirements

All reverse proxy configurations must include these essential settings:

### 1. Disable Proxy Buffering
**Most Important**: Buffering breaks real-time streaming
- Response data must pass through immediately
- No waiting for response completion

### 2. HTTP/1.1 Support
- Required for streaming connections
- Enables proper chunked transfer encoding

### 3. Connection Header Management
- Clear or properly manage Connection headers
- Prevent connection pooling interference

### 4. Disable Caching
- SSE responses should never be cached
- Each event stream is unique and live

## Server-Specific Configurations

### Nginx Configuration

Create a server block with SSE-specific settings:

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location /sse {
        proxy_pass http://localhost:8000/sse;
        
        # Essential SSE settings
        proxy_set_header Connection '';
        proxy_http_version 1.1;
        proxy_buffering off;
        proxy_cache off;
        chunked_transfer_encoding off;
        
        # Standard proxy headers
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

#### HTTPS Configuration

```nginx
server {
    listen 443 ssl http2;
    server_name your-domain.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location /sse {
        proxy_pass http://localhost:8000/sse;
        
        # Essential SSE settings
        proxy_set_header Connection '';
        proxy_http_version 1.1;
        proxy_buffering off;
        proxy_cache off;
        chunked_transfer_encoding off;
        
        # Headers for HTTPS
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
    }
}
```

### Caddy Configuration

Caddy has built-in SSE support with minimal configuration:

```caddyfile
your-domain.com {
    reverse_proxy /sse/* localhost:8000 {
        # Caddy handles SSE automatically
        # Optional: Add custom headers if needed
        header_up Host {http.reverse_proxy.upstream.hostport}
    }
}
```

#### Advanced Caddy Configuration

```caddyfile
your-domain.com {
    reverse_proxy /sse/* localhost:8000 {
        # Disable buffering for SSE
        flush_interval -1
        
        # Custom headers
        header_up Host {http.reverse_proxy.upstream.hostport}
        header_up X-Real-IP {http.request.remote}
        header_up X-Forwarded-Proto {http.request.scheme}
    }
}
```

### Traefik Configuration

#### Docker Compose with Traefik

```yaml
version: '3.8'
services:
  mcp-server:
    image: trufnetwork/postgres-mcp:latest
    command: ["--transport=sse", "--sse-host=0.0.0.0", "--access-mode=restricted"]
    environment:
      - DATABASE_URI=postgresql://kwild:password@postgres:5432/kwild
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.mcp.rule=Host(`your-domain.com`) && PathPrefix(`/sse`)"
      - "traefik.http.services.mcp.loadbalancer.server.port=8000"
      # SSE-specific labels
      - "traefik.http.middlewares.sse-headers.headers.customrequestheaders.Connection="
      - "traefik.http.routers.mcp.middlewares=sse-headers"

  traefik:
    image: traefik:v3.0
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

#### Traefik Static Configuration (traefik.yml)

```yaml
api:
  dashboard: true

entryPoints:
  web:
    address: ":80"
  websecure:
    address: ":443"

providers:
  docker:
    exposedByDefault: false

# Optional: Enable HTTPS with Let's Encrypt
certificatesResolvers:
  letsencrypt:
    acme:
      email: admin@your-domain.com
      storage: /letsencrypt/acme.json
      httpChallenge:
        entryPoint: web
```

### HAProxy Configuration

```
global
    daemon
    log stdout local0

defaults
    mode http
    timeout connect 5000ms
    timeout client 60000ms
    timeout server 60000ms
    
    # Important for SSE
    timeout client-fin 30s
    timeout server-fin 30s

frontend mcp_frontend
    bind *:80
    acl is_sse path_beg /sse
    use_backend mcp_sse if is_sse

backend mcp_sse
    # SSE requires HTTP/1.1
    http-request set-header Connection ""
    http-request set-header Host %[req.hdr(host)]
    
    # Disable buffering for streaming
    no option httpchk
    option http-server-close
    
    server mcp1 localhost:8000 check
```

### Apache HTTP Server Configuration

```apache
<VirtualHost *:80>
    ServerName your-domain.com
    
    <Location /sse>
        ProxyPass http://localhost:8000/sse
        ProxyPassReverse http://localhost:8000/sse
        
        # Essential for SSE
        ProxyPreserveHost On
        ProxyRequests Off
        
        # Disable buffering
        SetEnv proxy-nokeepalive 1
        SetEnv proxy-initial-not-pooled 1
        
        # Headers
        ProxyPassReverse /
        ProxyPassReverseRewrite /
    </Location>
</VirtualHost>

# Enable required modules
LoadModule proxy_module modules/mod_proxy.so
LoadModule proxy_http_module modules/mod_proxy_http.so
LoadModule headers_module modules/mod_headers.so
```

## MCP Server SSE Configuration

**Important**: TRUF.NETWORK uses the Kwil PostgreSQL Docker image (`ghcr.io/trufnetwork/kwil-postgres`) which is configured with `POSTGRES_HOST_AUTH_METHOD=trust` and automatically creates a `kwild` database with a `kwild` user. This matches the standard node setup described in the [Node Operator Guide](./node-operator-guide.md).

### Starting the MCP Server with SSE

#### Using Docker
```bash
docker run -p 8000:8000 \
  -e DATABASE_URI=postgresql://kwild@localhost:5432/kwild \
  trufnetwork/postgres-mcp \
  --access-mode=restricted \
  --transport=sse \
  --sse-host=0.0.0.0 \
  --sse-port=8000
```

#### Using Python directly
```bash
# Install the TRUF.NETWORK postgres-mcp
git clone https://github.com/trufnetwork/postgres-mcp.git
cd postgres-mcp
./install.sh

# Run with SSE transport
DATABASE_URI=postgresql://kwild@localhost:5432/kwild \
postgres-mcp --transport=sse --sse-host=0.0.0.0 --sse-port=8000 --access-mode=restricted
```

## Client Configuration

### MCP Client Configuration for SSE

Update your MCP client configuration to use the SSE endpoint:

#### Cursor (mcp.json)
```json
{
    "mcpServers": {
        "truf-postgres": {
            "type": "sse",
            "url": "http://your-domain.com/sse"
        }
    }
}
```

#### Cline (cline_mcp_settings.json)
```json
{
    "mcpServers": {
        "truf-postgres": {
            "type": "sse", 
            "url": "http://your-domain.com/sse"
        }
    }
}
```

#### Windsurf (mcp_config.json)
```json
{
    "mcpServers": {
        "truf-postgres": {
            "type": "sse",
            "serverUrl": "http://your-domain.com/sse"
        }
    }
}
```

#### Claude Desktop (claude_desktop_config.json)
**⚠️ Important**: Claude Desktop doesn't support direct SSE connections. You must use `mcp-remote` proxy tool.

```json
{
  "mcpServers": {
    "truf-postgres": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "http://your-domain.com/sse"
      ]
    }
  }
}
```

**Configuration Location:**
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Linux**: `~/.config/Claude/claude_desktop_config.json`

## Testing and Validation

### 1. Test SSE Endpoint Availability

```bash
# Test basic connectivity
curl -v http://your-domain.com/sse

# Test with SSE headers
curl -H "Accept: text/event-stream" \
     -H "Cache-Control: no-cache" \
     http://your-domain.com/sse
```

### 2. Verify Streaming Behavior

Check that responses stream immediately without buffering:

```bash
# Should show immediate response headers
curl -v -N -H "Accept: text/event-stream" http://your-domain.com/sse
```

### 3. Test MCP Client Connection

1. Configure your MCP client with the SSE URL
2. Verify the client can connect and list tools
3. Test executing queries through the SSE transport

### 4. Monitor Connection Health

```bash
# Check nginx access logs for SSE requests
tail -f /var/log/nginx/access.log | grep "/sse"

# Monitor MCP server logs
docker logs -f your-mcp-container
```

## Common Issues and Solutions

### Issue: Connection Timeouts

**Symptoms**: SSE connections drop after 30-60 seconds
**Solution**: Increase proxy timeouts

```nginx
# Nginx
location /sse {
    proxy_read_timeout 300s;
    proxy_send_timeout 300s;
    # ... other settings
}
```

```caddyfile
# Caddy
reverse_proxy /sse/* localhost:8000 {
    timeout 5m
}
```

### Issue: Responses Not Streaming

**Symptoms**: No data until connection closes
**Solution**: Ensure buffering is disabled

```nginx
# Nginx - Most critical setting
proxy_buffering off;

# Also verify chunked transfer encoding
chunked_transfer_encoding off;
```

### Issue: CORS Errors in Browser

**Symptoms**: Browser-based clients can't connect
**Solution**: Add CORS headers

```nginx
# Nginx
location /sse {
    add_header 'Access-Control-Allow-Origin' '*';
    add_header 'Access-Control-Allow-Headers' 'Origin, X-Requested-With, Content-Type, Accept, Authorization';
    add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS';
    
    # Handle preflight requests
    if ($request_method = 'OPTIONS') {
        return 204;
    }
    
    # ... other proxy settings
}
```

### Issue: SSL/TLS Problems

**Symptoms**: HTTPS connections fail
**Solution**: Verify certificate configuration and headers

```nginx
# Ensure proper forwarded headers for HTTPS
proxy_set_header X-Forwarded-Proto https;
proxy_set_header X-Forwarded-Port 443;
```

### Issue: Database Connection Errors

**Symptoms**: MCP tools fail with database errors
**Solution**: Verify DATABASE_URI and network connectivity

```bash
# Test database connectivity from MCP server (TRUF.NETWORK uses kwild user/database)
docker exec truf-mcp-server psql postgresql://kwild@localhost:5432/kwild -c "SELECT version();"

# Or connect to the TRUF.NETWORK postgres container directly
docker exec -it tn-postgres psql -U kwild -d kwild

### Issue: Windows IPv4/IPv6 Addressing Problems

**Symptoms**: Database connection timeouts on Windows, especially with SSH tunnels
**Solution**: Use explicit IPv4 addresses instead of localhost

```powershell
# Wrong: May resolve to IPv6 (::1) which breaks SSH tunnels
$env:DATABASE_URI = "postgresql://kwild@localhost:5432/kwild"

# Correct: Force IPv4 addressing  
$env:DATABASE_URI = "postgresql://kwild@127.0.0.1:5432/kwild"

# Start MCP server with IPv4
postgres-mcp --transport=sse --sse-host=127.0.0.1 --sse-port=8000

# Test connectivity
Test-NetConnection -ComputerName localhost -Port 5432
```

### Issue: Claude Desktop Direct SSE Configuration

**Symptoms**: Claude Desktop shows "Required" errors for missing "command" field
**Solution**: Claude Desktop requires `mcp-remote` proxy for SSE connections

```json
# Wrong: Direct SSE configuration not supported
{
  "mcpServers": {
    "truf-postgres": {
      "type": "sse",
      "url": "http://localhost:8000/sse"
    }
  }
}

# Correct: Use mcp-remote proxy
{
  "mcpServers": {
    "truf-postgres": {
      "command": "npx",
      "args": ["mcp-remote", "http://127.0.0.1:8000/sse"]
    }
  }
}
```

## Security Considerations

### 1. Access Control

```nginx
# Restrict access by IP
location /sse {
    allow 192.168.1.0/24;
    allow 10.0.0.0/8;
    deny all;
    
    # ... proxy settings
}
```

### 2. Rate Limiting

```nginx
# Nginx rate limiting
http {
    limit_req_zone $binary_remote_addr zone=sse:10m rate=10r/m;
    
    server {
        location /sse {
            limit_req zone=sse burst=5;
            # ... proxy settings
        }
    }
}
```

### 3. Authentication

Consider implementing authentication at the reverse proxy level:

```nginx
# Basic auth example
location /sse {
    auth_basic "MCP Server Access";
    auth_basic_user_file /etc/nginx/.htpasswd;
    
    # ... proxy settings  
}
```

### 4. Network Security

- Use HTTPS in production
- Implement firewall rules
- Use VPNs or private networks when possible
- Regular security updates for proxy software

## Production Deployment Example

Here's a complete production-ready nginx configuration:

```nginx
# /etc/nginx/sites-available/mcp-server
server {
    listen 443 ssl http2;
    server_name mcp.your-domain.com;

    # SSL Configuration
    ssl_certificate /etc/letsencrypt/live/mcp.your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/mcp.your-domain.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;

    # Security headers
    add_header X-Frame-Options DENY;
    add_header X-Content-Type-Options nosniff;
    add_header X-XSS-Protection "1; mode=block";
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;

    # Rate limiting
    limit_req zone=sse burst=10 nodelay;

    # MCP SSE endpoint
    location /sse {
        # Essential SSE configuration
        proxy_pass http://127.0.0.1:8000/sse;
        proxy_set_header Connection '';
        proxy_http_version 1.1;
        proxy_buffering off;
        proxy_cache off;
        chunked_transfer_encoding off;
        
        # Timeouts for long-lived connections
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
        proxy_connect_timeout 60s;
        
        # Headers
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-Port 443;
        
        # Access control (adjust as needed)
        allow 192.168.1.0/24;
        deny all;
    }
    
    # Health check endpoint
    location /health {
        proxy_pass http://127.0.0.1:8000/health;
        access_log off;
    }
}

# Redirect HTTP to HTTPS
server {
    listen 80;
    server_name mcp.your-domain.com;
    return 301 https://$host$request_uri;
}

# Rate limiting zone definition (place in http block)
# limit_req_zone $binary_remote_addr zone=sse:10m rate=5r/m;
```

## Configuration Examples

Ready-to-use configuration files and Docker Compose setups are available in the [`examples/mcp-reverse-proxy/`](./examples/mcp-reverse-proxy/) directory, including:

- **nginx.conf.example** - Production-ready nginx configuration
- **Caddyfile.example** - Caddy configuration with automatic HTTPS  
- **traefik.yml.example** - Traefik static/dynamic configuration
- **docker-compose.sse.yaml** - Complete Docker Compose setup with multiple reverse proxy options

## Next Steps

After implementing reverse proxy configuration:

1. **Monitor Performance**: Track connection counts, response times, and error rates
2. **Set Up Logging**: Configure detailed logging for debugging and monitoring  
3. **Implement Monitoring**: Use tools like Grafana/Prometheus for metrics
4. **Plan Scaling**: Consider load balancing for high-traffic scenarios
5. **Security Audits**: Regular security reviews and updates

For additional support with MCP server deployment, consult the [Node Operator Guide](./node-operator-guide.md) and [MCP Server Documentation](./mcp-server.md).
