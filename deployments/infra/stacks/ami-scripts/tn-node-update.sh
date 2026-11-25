#!/bin/bash
# Installs a TN node release on a node built from the TN AMI.
#
#   sudo tn-node-update v2.6.9   runs the release tagged v2.6.9
#   sudo tn-node-update          pulls the release the node runs now
#
# The tag is stored as TN_NODE_TAG in /opt/tn/.env. The script pulls that node
# image and runs docker compose up for tn-node, which recreates the node
# containers only when their image or configuration changed. Postgres and the
# MCP server keep running.
set -euo pipefail

TN_DIR="${TN_DIR:-/opt/tn}"
ENV_FILE="$TN_DIR/.env"

if [ $# -gt 1 ]; then
  echo "Usage: tn-node-update [release tag, for example v2.6.9]" >&2
  exit 1
fi
if [ ! -f "$ENV_FILE" ]; then
  echo "Error: $ENV_FILE not found. Run tn-node-configure first." >&2
  exit 1
fi

CURRENT=$(grep '^TN_NODE_TAG=' "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true)
if [ $# -eq 1 ]; then
  TAG=$1
else
  TAG=$CURRENT
  if [ -z "$TAG" ]; then
    echo "Error: $ENV_FILE has no TN_NODE_TAG. Pass the release tag, for example: tn-node-update v2.6.9" >&2
    exit 1
  fi
fi
# Release tags are on https://github.com/trufnetwork/node/releases. latest is
# refused so that the node runs the release its operator chose.
if ! [[ $TAG =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || [ "$TAG" = latest ]; then
  echo "Error: '$TAG' is not a release tag. Pass one from https://github.com/trufnetwork/node/releases, for example v2.6.9" >&2
  exit 1
fi

cd "$TN_DIR"
# Pull before .env changes, so a tag with no image leaves the node as it was.
sudo -u tn env TN_NODE_TAG="$TAG" docker compose pull tn-postgres-roles tn-node

if [ "$TAG" != "$CURRENT" ]; then
  if grep -q '^TN_NODE_TAG=' "$ENV_FILE"; then
    sed -i "s/^TN_NODE_TAG=.*/TN_NODE_TAG=$TAG/" "$ENV_FILE"
  else
    echo "TN_NODE_TAG=$TAG" >> "$ENV_FILE"
  fi
  chown tn:tn "$ENV_FILE"
  chmod 600 "$ENV_FILE"
fi

sudo -u tn docker compose up -d tn-node
echo "tn-node runs ghcr.io/trufnetwork/node:$TAG"
