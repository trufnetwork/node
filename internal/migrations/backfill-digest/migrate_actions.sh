#!/usr/bin/env bash

# Enable strict mode
set -euo pipefail
IFS=$'\n\t'

# this script will use exec sql to migrate all the files in the correct order for the schema update:

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"  
base_dir="$(cd "$SCRIPT_DIR/.." && pwd)"  
# Test key
: "${PRIVATE_KEY:="0000000000000000000000000000000000000000000000000000000000000001"}"  
: "${PROVIDER:=http://localhost:8484}"  

files=(
# first run the needed new schema updates
$base_dir/019-digest-schema.sql
# no need for old schema updates
# $base_dir/000-extensions.sql
# $base_dir/000-initial-data.sql
$base_dir/001-common-actions.sql
$base_dir/002-authorization.sql
$base_dir/003-primitive-insertion.sql
$base_dir/004-composed-taxonomy.sql
$base_dir/005-primitive-query.sql
$base_dir/006-composed-query.sql
$base_dir/007-composed-query-derivate.sql
$base_dir/008-public-query.sql
$base_dir/009-truflation-query.sql
$base_dir/010-get-latest-write-activity.sql
$base_dir/011-get-database-size.sql
# $base_dir/012-roles-schema.sql
$base_dir/013-role-actions.sql
$base_dir/014-role-manager-actions.sql
# $base_dir/015-system-roles-bootstrap.sql
$base_dir/016-taxonomy-query-actions.sql
# $base_dir/017-normalize-tables.sql
$base_dir/018-fix-array-ordering-get-stream-ids.sql
$base_dir/020-digest-actions.sql
# $base_dir/901-utilities.sql
) 

# Preflight checks
echo "🔍 Performing preflight checks..."

# Check if kwil-cli is available in PATH
if ! command -v kwil-cli >/dev/null 2>&1; then
    echo "❌ Error: kwil-cli not found in PATH"
    echo "   Please ensure kwil-cli is installed and available in your PATH"
    exit 1
fi

# Validate required environment variables
if [[ -z "${PRIVATE_KEY:-}" ]]; then
    echo "❌ Error: PRIVATE_KEY is not set"
    exit 1
fi

if [[ -z "${PROVIDER:-}" ]]; then
    echo "❌ Error: PROVIDER is not set"
    exit 1
fi

echo "✅ Preflight checks passed"
echo

# run the migrations
echo "🚀 Starting migrations..."
for file in "${files[@]}"; do
    # Skip commented-out files (those starting with #)
    if [[ "$file" =~ ^[[:space:]]*# ]]; then
        continue
    fi

    # Check if migration file exists
    if [[ ! -f "$file" ]]; then
        echo "❌ Error: Migration file not found: $file"
        exit 1
    fi

    echo "📄 Running $file"

    # Execute migration and check exit status
    if kwil-cli exec-sql --file "$file" --private-key "$PRIVATE_KEY" --provider "$PROVIDER" --sync; then
        :
    else
        rc=$?
        echo "❌ Migration failed: $file"
        echo "   Command: kwil-cli exec-sql --file \"$file\" --private-key \"***\" --provider \"$PROVIDER\" --sync"
        echo "   Exit code: $rc"
        echo
        echo "💡 Troubleshooting suggestions:"
        echo "   - Check if the database is running and accessible at $PROVIDER"
        echo "   - Verify the private key is correct"
        echo "   - Re-run with verbose logging: kwil-cli exec-sql --verbose --file \"$file\" --private-key \"***\" --provider \"$PROVIDER\" --sync"
        exit "$rc"
    fi
    fi

    echo "✅ Completed $file"
    echo
done

echo "🎉 All migrations completed successfully!"