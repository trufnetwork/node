# Get all .sql files in ./internal/migrations folder and run them with kwil-cli exec-sql --file /path/to/file.sql
for file in ./internal/migrations/*.sql; do
    echo "Running $file"
    kwil-cli exec-sql --file $file
done