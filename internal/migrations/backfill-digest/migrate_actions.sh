# this script will use exec sql to migrate all the files in the correct order for the schema update:

base_dir="/home/outerlook/Documents/usher/trufnetwork/node/internal/migrations"
PRIVATE_KEY=0000000000000000000000000000000000000000000000000000000000000001
PROVIDER=http://localhost:8484

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

# run the migrations
for file in "${files[@]}"; do
    echo "Running $file"
    kwil-cli exec-sql --file "$file" --private-key "$PRIVATE_KEY" --provider "$PROVIDER" --sync
done