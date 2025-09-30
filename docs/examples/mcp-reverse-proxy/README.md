# MCP Server Reverse Proxy Configuration Examples

This directory contains ready-to-use configuration examples for deploying the TRUF.NETWORK MCP server with SSE (Server-Sent Events) transport behind various reverse proxy servers.

📖 **Main Documentation**: See [MCP Server Reverse Proxy Configuration](../../mcp-reverse-proxy.md) for detailed setup instructions and explanations.

## 📁 Files Overview

| File | Description |
|------|-------------|
| `nginx.conf.example` | Complete nginx configuration with HTTPS, security headers, and SSE optimization |
| `Caddyfile.example` | Caddy configuration with automatic HTTPS and built-in SSE support |
| `Caddyfile.windows.example` | Windows-specific Caddy configuration with IPv4 addressing |
| `traefik.yml.example` | Traefik static/dynamic configuration with Docker Compose integration |
| `docker-compose.sse.yaml` | Complete Docker Compose setup with multiple reverse proxy options |
| `claude-desktop-config.example` | Claude Desktop configuration examples |
| `README.md` | This documentation file |

## 🚀 Quick Start

### Option 1: Using Docker Compose (Recommended)

1. **Copy the Docker Compose file:**
   ```bash
   cp docker-compose.sse.yaml docker-compose.yml
   ```

2. **Create environment file:**
   ```bash
   cat > .env << EOF
   # TRUF.NETWORK uses ghcr.io/trufnetwork/kwil-postgres image which auto-creates kwild user/database
   # Note: No password needed - uses POSTGRES_HOST_AUTH_METHOD=trust
   DOMAIN=mcp.your-domain.com
   ACME_EMAIL=admin@your-domain.com
   EOF
   ```

3. **Start with your preferred reverse proxy:**
   ```bash
   # Option A: Nginx
   docker compose --profile nginx up -d
   
   # Option B: Caddy (automatic HTTPS)
   docker compose --profile caddy up -d
   
   # Option C: Traefik (container orchestration)
   docker compose --profile traefik up -d
   ```

### Option 2: Windows Native Testing

**📋 Complete Windows testing guide:** [Windows MCP Testing](../../windows-mcp-testing.md)

1. **Copy Windows-specific files:**
   ```powershell
   cp Caddyfile.windows.example Caddyfile
   cp claude-desktop-config.windows.example claude-config.json
   ```

2. **Start MCP server with IPv4 addressing:**
   ```powershell
   $env:DATABASE_URI = "postgresql://kwild@127.0.0.1:5432/kwild"
   postgres-mcp --transport=sse --sse-host=127.0.0.1 --sse-port=8000
   ```

3. **Start Caddy reverse proxy:**
   ```powershell
   caddy run
   ```

4. **Test both endpoints:**
   ```powershell
   curl.exe -H "Accept: text/event-stream" http://127.0.0.1:8000/sse
   curl.exe -H "Accept: text/event-stream" http://127.0.0.1:8080/sse
   ```

### Option 3: Manual Configuration

1. **Choose your reverse proxy configuration file**
2. **Update domain names and paths for your environment**
3. **Deploy according to your reverse proxy's documentation**

## 🔧 Configuration Requirements

### Critical SSE Settings

All reverse proxy configurations **MUST** include these settings for SSE to work:

```nginx
# Nginx example
proxy_set_header Connection '';
proxy_http_version 1.1;
proxy_buffering off;
proxy_cache off;
chunked_transfer_encoding off;
```

```caddyfile
# Caddy example (mostly automatic)
reverse_proxy /sse/* localhost:8000 {
    flush_interval -1
}
```

### Why These Settings Matter

- **`proxy_buffering off`**: Most critical - prevents response buffering that breaks streaming
- **`proxy_http_version 1.1`**: Required for persistent connections and chunked encoding
- **`Connection ''`**: Clears connection header to prevent pooling issues
- **`proxy_cache off`**: SSE responses must never be cached

## 🌐 Supported Reverse Proxies

### Nginx ✅
- **Complexity**: Medium
- **SSE Support**: Excellent with proper configuration
- **HTTPS**: Manual certificate management
- **Best for**: High-traffic production environments

### Caddy ✅
- **Complexity**: Low
- **SSE Support**: Built-in, minimal configuration needed
- **HTTPS**: Automatic with Let's Encrypt
- **Best for**: Simple deployments, automatic HTTPS

### Traefik ✅
- **Complexity**: Medium-High
- **SSE Support**: Good with proper middleware
- **HTTPS**: Automatic with multiple providers
- **Best for**: Container orchestration, microservices

### HAProxy ✅
- **Complexity**: High
- **SSE Support**: Good with timeout configuration
- **HTTPS**: Requires separate termination
- **Best for**: Load balancing, high availability

### Apache ⚠️
- **Complexity**: High
- **SSE Support**: Possible but requires extensive configuration
- **HTTPS**: Manual configuration
- **Best for**: Legacy environments

## 🧪 Testing Your Setup

### 1. Test SSE Endpoint

```bash
# Basic connectivity test
curl -v http://your-domain.com/sse

# SSE-specific test
curl -H "Accept: text/event-stream" \
     -H "Cache-Control: no-cache" \
     http://your-domain.com/sse
```

### 2. Test MCP Client

Configure your MCP client:

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

### 3. Verify Streaming

Look for immediate response headers (no buffering):

```bash
curl -v -N -H "Accept: text/event-stream" http://your-domain.com/sse
```

## 🔒 Security Considerations

### Production Checklist

- [ ] Enable HTTPS with valid certificates
- [ ] Implement rate limiting
- [ ] Configure access control (IP whitelist, authentication)
- [ ] Set up security headers
- [ ] Enable logging and monitoring
- [ ] Regular security updates
- [ ] Network segmentation (private networks)

### Example Security Headers

```nginx
add_header X-Frame-Options DENY;
add_header X-Content-Type-Options nosniff;
add_header X-XSS-Protection "1; mode=block";
add_header Strict-Transport-Security "max-age=31536000; includeSubDomains";
```

## 🚨 Common Issues

### Issue: SSE Connection Drops
**Solution**: Increase proxy timeouts
```nginx
proxy_read_timeout 300s;
proxy_send_timeout 300s;
```

### Issue: No Streaming (Buffered Response)
**Solution**: Verify `proxy_buffering off` is set

### Issue: CORS Errors  
**Solution**: Add CORS headers for browser-based clients
```nginx
add_header 'Access-Control-Allow-Origin' '*';
```

### Issue: SSL/TLS Problems
**Solution**: Check certificate paths and forwarded headers

## 📊 Monitoring

### Nginx Access Logs
```bash
tail -f /var/log/nginx/access.log | grep "/sse"
```

### MCP Server Logs
```bash
docker logs -f your-mcp-container
```

### Health Checks
Add health check endpoints to your configuration:
```nginx
location /health {
    proxy_pass http://localhost:8000/health;
    access_log off;
}
```

## 📚 Additional Resources

- [MCP Server Documentation](../../mcp-server.md)
- [Complete Reverse Proxy Guide](../../mcp-reverse-proxy.md)
- [Node Operator Guide](../../node-operator-guide.md)
- [TRUF.NETWORK Documentation](https://docs.truf.network)

## 🆘 Support

If you encounter issues:

1. Check the [troubleshooting section](../../mcp-reverse-proxy.md#common-issues-and-solutions)
2. Verify your configuration against the examples
3. Test with curl commands provided above
4. Check logs for error messages
5. Open an issue on the [TRUF.NETWORK repository](https://github.com/trufnetwork/node)

## 🤝 Contributing

Have a working configuration for another reverse proxy? Please contribute:

1. Create a new configuration file
2. Test thoroughly
3. Document any special requirements  
4. Submit a pull request

---

**⚡ Quick Deployment Commands:**

```bash
# Clone and setup
git clone https://github.com/trufnetwork/node.git
cd node/docs/examples/mcp-reverse-proxy

# Configure environment (ghcr.io/trufnetwork/kwil-postgres auto-creates kwild user/database)  
cat > .env << EOF
DOMAIN=mcp.your-domain.com
ACME_EMAIL=admin@your-domain.com
EOF

# Deploy with Caddy (easiest)
docker compose --profile caddy up -d

# Check status
docker compose ps
docker compose logs -f mcp-server

# Test
curl -H "Accept: text/event-stream" http://your-domain.com/sse
```
