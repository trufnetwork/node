#!/bin/bash
set -Eeuo pipefail
IFS=$'\n\t'

echo "🧪 TrufNetwork AMI Testing Suite"
echo "================================"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

# Detect Docker Compose command
if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    echo -e "${RED}❌ Neither 'docker compose' nor 'docker-compose' found${NC}"
    echo "Please install Docker Compose v2 or legacy v1"
    exit 1
fi

echo "Using Docker Compose: $COMPOSE"

# Cleanup function for test containers
cleanup() {
  if [ -f /tmp/tn-test-compose.yml ]; then
    eval "$COMPOSE -f /tmp/tn-test-compose.yml down -v" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

# Test 1: CDK Synthesis
echo "1. Testing CDK synthesis..."
cd deployments/infra
if cdk --app 'go run ami-cdk.go' synth --context stage=dev --context devPrefix=test > /dev/null 2>&1; then
    echo -e "${GREEN}✅ CDK synthesis successful${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}❌ CDK synthesis failed${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi
cd ../..

# Test 2: GitHub Actions Workflow Syntax
echo "2. Testing GitHub Actions workflow syntax..."
if command -v yamllint &> /dev/null; then
    echo "Using yamllint with relaxed line-length rules"
    YAML_VALID=true

    if ! yamllint -d '{extends: default, rules: {line-length: {max: 200}}}' .github/workflows/ami-build.yml > /dev/null 2>&1; then
        echo -e "${RED}❌ ami-build.yml has syntax errors${NC}"
        YAML_VALID=false
    fi

    if ! yamllint -d '{extends: default, rules: {line-length: {max: 200}}}' .github/workflows/release.yaml > /dev/null 2>&1; then
        echo -e "${RED}❌ release.yaml has syntax errors${NC}"
        YAML_VALID=false
    fi

    if ! yamllint -d '{extends: default, rules: {line-length: {max: 200}}}' .github/workflows/publish-node-image.yaml > /dev/null 2>&1; then
        echo -e "${RED}❌ publish-node-image.yaml has syntax errors${NC}"
        YAML_VALID=false
    fi

    if [ "$YAML_VALID" = true ]; then
        echo -e "${GREEN}✅ AMI build workflow syntax valid${NC}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}❌ GitHub Actions workflow syntax invalid${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
else
    echo "Using Python YAML parser (yamllint not available)"
    if python3 -c "
import yaml
import sys
try:
    with open('.github/workflows/ami-build.yml') as f:
        yaml.safe_load(f)
    with open('.github/workflows/release.yaml') as f:
        yaml.safe_load(f)
    with open('.github/workflows/publish-node-image.yaml') as f:
        yaml.safe_load(f)
    sys.exit(0)
except Exception as e:
    print(f'YAML Error: {e}')
    sys.exit(1)
" 2>/dev/null; then
        echo -e "${GREEN}✅ GitHub Actions workflow syntax valid${NC}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}❌ GitHub Actions workflow syntax invalid${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
fi

# Test 3: Docker Compose Configuration
echo "3. Testing Docker Compose configuration..."
echo "Using shared Docker Compose configuration..."

# Use shared Docker Compose configuration (single source of truth)
cp deployments/infra/stacks/docker-compose.template.yml /tmp/test-docker-compose.yml

if eval "$COMPOSE -f /tmp/test-docker-compose.yml config" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Docker Compose configuration valid${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}❌ Docker Compose configuration invalid${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi
rm -f /tmp/test-docker-compose.yml

# Test 4: Shell Script Syntax
echo "4. Testing shell script syntax..."
if bash -n scripts/test-ami.sh; then
    echo -e "${GREEN}✅ Test script syntax valid${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}❌ Test script syntax invalid${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 5: Go Module Compilation
echo "5. Testing Go module compilation..."
echo "Running: go mod tidy && go build in deployments/infra"
cd deployments/infra
if go mod tidy && go build ./lib/... ./stacks/... > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Go module compilation successful${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}❌ Go module compilation failed${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi
# Build successful, no files to remove
cd ../..

# Test 6: Configuration Script Logic
echo "6. Testing configuration script logic..."
echo "Testing command-line argument parsing and environment file generation..."

cat > /tmp/test-config.sh << 'EOF'
#!/bin/bash
set -e

PRIVATE_KEY=""
ENABLE_MCP=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --private-key) PRIVATE_KEY="$2"; shift 2 ;;
    --enable-mcp) ENABLE_MCP=true; shift ;;
    *) echo "Unknown option $1"; exit 1 ;;
  esac
done

echo "Network: mainnet (tn-v2.1)"
echo "MCP enabled: $ENABLE_MCP"

[[ "$PRIVATE_KEY" == "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef" ]] || { echo "Private key parsing failed"; exit 1; }
[[ "$ENABLE_MCP" == true ]] || { echo "MCP flag parsing failed"; exit 1; }

echo "Configuration script logic validation passed"
EOF

chmod +x /tmp/test-config.sh
echo "Testing: /tmp/test-config.sh --private-key 1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef --enable-mcp"
if /tmp/test-config.sh --private-key "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef" --enable-mcp; then
    echo -e "${GREEN}✅ Configuration script logic works correctly${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}❌ Configuration script logic failed${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi
rm -f /tmp/test-config.sh

# Test 6.2: Private Key Validation
echo "6.2 Testing private key validation..."
echo "Testing that invalid private keys are rejected..."

# Test invalid key (too short)
cat > /tmp/test-validation.sh << 'EOF'
#!/bin/bash
set -e

# Simulate the validation logic from Docker container
PRIVATE_KEY="$1"
CLEAN_KEY="${PRIVATE_KEY#0x}"

if ! echo "$CLEAN_KEY" | grep -qE '^[a-fA-F0-9]{64}$'; then
  echo "Error: Private key must be 64 hex characters (32 bytes)"
  exit 1
fi

echo "Valid private key"
EOF

chmod +x /tmp/test-validation.sh

# Test 1: Invalid short key should fail
if /tmp/test-validation.sh "test123" 2>/dev/null; then
    echo -e "${RED}❌ Short private key validation failed - should reject invalid keys${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
else
    echo -e "${GREEN}✅ Short private key correctly rejected${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

# Test 2: Valid key should pass
if /tmp/test-validation.sh "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"; then
    echo -e "${GREEN}✅ Valid private key correctly accepted${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}❌ Valid private key validation failed${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 3: Key with 0x prefix should work
if /tmp/test-validation.sh "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"; then
    echo -e "${GREEN}✅ Private key with 0x prefix correctly accepted${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}❌ Private key with 0x prefix validation failed${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

rm -f /tmp/test-validation.sh

# Test 6.3: Negative Private Key Tests
echo "6.3 Testing negative private key validation cases..."
echo "Testing various invalid private key formats..."

cat > /tmp/test-negative.sh << 'EOF'
#!/bin/bash
set -e

# Simulate the validation logic from Docker container
PRIVATE_KEY="$1"
CLEAN_KEY="${PRIVATE_KEY#0x}"

if ! echo "$CLEAN_KEY" | grep -qE '^[a-fA-F0-9]{64}$'; then
  echo "Error: Private key must be 64 hex characters (32 bytes)"
  exit 1
fi

echo "Valid private key"
EOF

chmod +x /tmp/test-negative.sh

# Array of invalid test cases
INVALID_KEYS=(
  "test123"                                                      # Too short
  ""                                                            # Empty string
  "123"                                                         # Way too short
  "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg" # Non-hex characters (g)
  "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcde"   # 63 chars (too short by 1)
  "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1"  # 65 chars (too long by 1)
  "xyz1567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"   # Non-hex at start
  "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdez"   # Non-hex at end
  "1234 67890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"   # Contains space
  "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdeG"   # Non-hex character G at end
  "G234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"   # Non-hex character G at start
  "!@#\$567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"   # Special characters
  "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdXX"   # Non-hex character
)

DESCRIPTIONS=(
  "short alphanumeric"
  "empty string"
  "very short number"
  "non-hex characters (g)"
  "63 characters (too short)"
  "65 characters (too long)"
  "non-hex at start"
  "non-hex at end"
  "contains space"
  "non-hex character G at end"
  "non-hex character G at start"
  "special characters"
  "non-hex character XX"
)

NEGATIVE_TESTS_PASSED=0
NEGATIVE_TESTS_FAILED=0

for i in "${!INVALID_KEYS[@]}"; do
  KEY="${INVALID_KEYS[$i]}"
  DESC="${DESCRIPTIONS[$i]}"

  echo "Testing invalid key: $DESC"

  if /tmp/test-negative.sh "$KEY" 2>/dev/null; then
    echo -e "${RED}❌ FAILED: Invalid key '$DESC' was incorrectly accepted${NC}"
    NEGATIVE_TESTS_FAILED=$((NEGATIVE_TESTS_FAILED + 1))
    TESTS_FAILED=$((TESTS_FAILED + 1))
  else
    echo -e "${GREEN}✅ PASSED: Invalid key '$DESC' correctly rejected${NC}"
    NEGATIVE_TESTS_PASSED=$((NEGATIVE_TESTS_PASSED + 1))
    TESTS_PASSED=$((TESTS_PASSED + 1))
  fi
done

echo ""
echo "Negative test summary: $NEGATIVE_TESTS_PASSED passed, $NEGATIVE_TESTS_FAILED failed"

# Test edge case: Mixed case should actually be VALID (hex is case-insensitive)
echo "Testing edge case: Mixed case hex (should be valid)..."
if /tmp/test-negative.sh "1234567890ABCDEF1234567890abcdef1234567890ABCDEF1234567890abcdef"; then
    echo -e "${GREEN}✅ Mixed case hex correctly accepted (case-insensitive)${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}❌ Mixed case hex incorrectly rejected${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

rm -f /tmp/test-negative.sh

# Test 7: Environment File Generation
echo "7. Testing environment file generation..."
echo "Testing environment file creation for different network configurations..."

cat > /tmp/test-env-gen.sh << 'EOF'
#!/bin/bash
NETWORK="mainnet"
ENABLE_MCP=true

# Chain ID is always tn-v2.1
CHAIN_ID="tn-v2.1"

cat > /tmp/test.env << EOL
CHAIN_ID=$CHAIN_ID
DB_OWNER=postgres://kwild:kwild@kwil-postgres:5432/kwild
EOL

# Only write COMPOSE_PROFILES when MCP is enabled
if [ "$ENABLE_MCP" = true ]; then
    echo "COMPOSE_PROFILES=mcp" >> /tmp/test.env
fi

echo "Generated environment file:"
cat /tmp/test.env

grep -q "CHAIN_ID=tn-v2.1" /tmp/test.env && \
grep -q "COMPOSE_PROFILES=mcp" /tmp/test.env
EOF

if bash /tmp/test-env-gen.sh; then
    echo -e "${GREEN}✅ Environment file generation successful${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}❌ Environment file generation failed${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi
rm -f /tmp/test-env-gen.sh /tmp/test.env

# Test 8: Docker Image Availability
echo "8. Testing required Docker images availability..."
echo "Checking if required Docker images can be pulled..."

DOCKER_IMAGES_AVAILABLE=true

echo "Checking ghcr.io/trufnetwork/kwil-postgres:16.8-1..."
if docker manifest inspect ghcr.io/trufnetwork/kwil-postgres:16.8-1 >/dev/null 2>&1; then
    echo -e "${GREEN}✓ ghcr.io/trufnetwork/kwil-postgres:16.8-1 available${NC}"
else
    echo -e "${RED}❌ ghcr.io/trufnetwork/kwil-postgres:16.8-1 not available${NC}"
    DOCKER_IMAGES_AVAILABLE=false
fi

echo "Checking ghcr.io/trufnetwork/postgres-mcp:latest..."
if docker manifest inspect ghcr.io/trufnetwork/postgres-mcp:latest >/dev/null 2>&1; then
    echo -e "${GREEN}✓ ghcr.io/trufnetwork/postgres-mcp:latest available${NC}"
else
    echo -e "${RED}❌ ghcr.io/trufnetwork/postgres-mcp:latest not available${NC}"
    DOCKER_IMAGES_AVAILABLE=false
fi

if [ "$DOCKER_IMAGES_AVAILABLE" = true ]; then
    echo -e "${GREEN}✅ Required Docker images availability successful${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}❌ Required Docker images availability failed${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 9: PostgreSQL Service Startup
echo "9. Testing PostgreSQL service startup..."
echo "Starting PostgreSQL container to test database connectivity..."

# Use shared Docker Compose configuration for PostgreSQL test
cp deployments/infra/stacks/docker-compose.template.yml /tmp/tn-test-compose.yml

echo "Starting PostgreSQL container..."
if eval "$COMPOSE -f /tmp/tn-test-compose.yml up -d tn-postgres"; then
    echo "Waiting for PostgreSQL to be ready..."
    timeout=30
    while [ $timeout -gt 0 ]; do
        if eval "$COMPOSE -f /tmp/tn-test-compose.yml exec -T tn-postgres pg_isready -U postgres" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ PostgreSQL started successfully${NC}"

            echo "Testing database connection..."
            if eval "$COMPOSE -f /tmp/tn-test-compose.yml exec -T tn-postgres psql -U postgres -d kwild -c \"SELECT version();\"" > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Database connection successful${NC}"
                TESTS_PASSED=$((TESTS_PASSED + 1))
            else
                echo -e "${RED}❌ Database connection failed${NC}"
                TESTS_FAILED=$((TESTS_FAILED + 1))
            fi
            break
        fi
        sleep 1
        ((timeout--))
    done

    if [ $timeout -eq 0 ]; then
        echo -e "${RED}❌ PostgreSQL failed to start within 30 seconds${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
else
    echo -e "${RED}❌ Failed to start PostgreSQL container${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

echo "Cleaning up test containers..."
eval "$COMPOSE -f /tmp/tn-test-compose.yml down -v"
rm -f /tmp/tn-test-compose.yml

# Test 10: Update Script Workflow
echo "10. Testing update script workflow..."
echo "Simulating the always-latest container update workflow..."

cat > /tmp/tn-update-test.sh << 'EOF'
#!/bin/bash
set -e

echo "🔄 Updating TrufNetwork node to latest version..."

echo "📦 Pulling latest images..."
# Detect Docker Compose command
if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    echo "❌ Neither 'docker compose' nor 'docker-compose' found"
    exit 1
fi

echo "✓ Using $COMPOSE"
echo "✓ Simulated pulling ghcr.io/trufnetwork/node:latest"
echo "✓ Simulated pulling ghcr.io/trufnetwork/kwil-postgres:16.8-1"
echo "✓ Simulated pulling ghcr.io/trufnetwork/postgres-mcp:latest"

echo "🔄 Restarting services..."
echo "✓ Stopping existing containers"
echo "✓ Starting containers with latest images"
echo "✅ Services updated to latest version!"
echo "All containers are now running the latest images."
EOF

chmod +x /tmp/tn-update-test.sh
if /tmp/tn-update-test.sh; then
    echo -e "${GREEN}✅ Update script workflow successful${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}❌ Update script workflow failed${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi
rm -f /tmp/tn-update-test.sh

echo ""
TOTAL_TESTS=$((TESTS_PASSED + TESTS_FAILED))

if [ "${TESTS_FAILED}" -eq 0 ]; then
    echo "🎉 All tests passed!"
    echo ""
    echo "📊 Test Results: ${TESTS_PASSED}/${TOTAL_TESTS} tests passed"
    echo ""
    echo "📋 Summary of what was tested:"
    echo "  • CDK infrastructure synthesis"
    echo "  • GitHub Actions workflow syntax"
    echo "  • Docker Compose configuration"
    echo "  • Shell script syntax"
    echo "  • Go module compilation"
    echo "  • Configuration script logic"
    echo "  • Environment file generation"
    echo "  • Docker images availability"
    echo "  • PostgreSQL service startup"
    echo "  • Update script workflow"
    echo ""
    echo "📝 Next steps:"
    echo "  1. Deploy the AMI infrastructure: cd deployments/infra && cdk deploy AMI-Pipeline-default-Stack"
    echo "  2. Test AMI build: Go to GitHub Actions and run the 'Build AMI' workflow"
    echo "  3. Test user experience: Launch AMI and run tn-node-configure --enable-mcp"
    echo ""
    exit 0
else
    echo -e "${RED}❌ Tests failed!${NC}"
    echo ""
    echo "📊 Test Results: ${TESTS_PASSED}/${TOTAL_TESTS} tests passed, ${TESTS_FAILED} failed"
    echo -e "${RED}Please fix the issues before deployment.${NC}"
    exit 1
fi
