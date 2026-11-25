#!/bin/bash
set -e
TN_DIR="${TN_DIR:-/opt/tn}"
PRIVATE_KEY=""
ENABLE_MCP=false
NETWORK=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --private-key) PRIVATE_KEY="$2"; shift 2;;
    --enable-mcp) ENABLE_MCP=true; shift;;
    --network) NETWORK="$2"; shift 2;;
    *) echo "Unknown option"; exit 1;;
  esac
done
RECONFIGURE=false
[ -f "$TN_DIR/.env" ] && RECONFIGURE=true
if [ -n "$NETWORK" ]; then
  CHAIN_ID="$NETWORK"
  NETWORK_TYPE="custom"
else
  CHAIN_ID="tn-v2.1"
  NETWORK_TYPE="mainnet"
fi
cd "$TN_DIR"
# The node release this AMI was built with. A reconfigure keeps the release
# that tn-node-update set.
NODE_TAG=$(cat ami-node-tag)
if [ "$RECONFIGURE" = true ]; then
  [ -n "$PRIVATE_KEY" ] && { echo "Error: Cannot change key"; exit 1; }
  [ -n "$NETWORK" ] && { echo "Error: Cannot change network"; exit 1; }
  grep -q "TN_PRIVATE_KEY=" .env && EXISTING_KEY=$(grep "TN_PRIVATE_KEY=" .env | cut -d'=' -f2)
  grep -q "CHAIN_ID=" .env && CHAIN_ID=$(grep "CHAIN_ID=" .env | cut -d'=' -f2)
  grep -q "NETWORK_TYPE=" .env && NETWORK_TYPE=$(grep "NETWORK_TYPE=" .env | cut -d'=' -f2)
  grep -q "^TN_NODE_TAG=" .env && NODE_TAG=$(grep "^TN_NODE_TAG=" .env | tail -n 1 | cut -d'=' -f2)
  sudo systemctl stop tn-node || true
  sudo -u tn docker compose down || true
fi
cat > .env << ENVEOF
TN_NODE_TAG=$NODE_TAG
CHAIN_ID=$CHAIN_ID
NETWORK_TYPE=$NETWORK_TYPE
ENVEOF
[ "$ENABLE_MCP" = true ] && echo "COMPOSE_PROFILES=mcp" >> .env
if [ "$RECONFIGURE" = false ] && [ -n "$PRIVATE_KEY" ]; then
  echo "TN_PRIVATE_KEY=$PRIVATE_KEY" >> .env
elif [ "$RECONFIGURE" = true ] && [ -n "$EXISTING_KEY" ]; then
  echo "TN_PRIVATE_KEY=$EXISTING_KEY" >> .env
fi
sudo chown tn:tn .env
sudo chmod 600 .env
sudo systemctl daemon-reload
sudo systemctl enable tn-node
sudo systemctl start tn-node
MAX_WAIT=60
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
  sudo -u tn docker compose ps --status running 2>/dev/null | grep -q tn-node && break
  sleep 2
  ELAPSED=$((ELAPSED + 2))
done
echo "Done. Status: $(sudo systemctl is-active tn-node)"
if [ "$ENABLE_MCP" = true ]; then
  echo "MCP: http://$(curl -s --max-time 2 http://169.254.169.254/latest/meta-data/public-ipv4 || echo localhost):8000/sse"
fi
