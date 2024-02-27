# create temp_csv folder
rm -rf ./temp_csv
mkdir -p ./temp_csv

# for each file on ./raw_csv, create temp file that have cleaned data
# the header `Date,CPI_value,Yahoo_value,Nielsen_value,Numbeo_value` will be kept
# for each CPI_value, Yahoo_value, Nielsen_value, Numbeo_value, multiply by 1000 to remove decimal
for file in ./raw_csv/*.csv; do
  echo "Processing $file"
  awk -F, 'BEGIN {OFS=","} {if (NR==1) print; else print $1, $2*1000, $3*1000, $4*1000, $5*1000}' "$file" > ./temp_csv/$(basename "$file")
done

# seed the database
for file in ./temp_csv/*.csv; do
  echo "Seeding $file"
  if [ -n "$(echo "$file" | grep "Meats")" ]; then
    echo "Seeding meat"
    kwil-cli database batch --path "$file" --action add_record --name meat --map-inputs "Date:date_value,CPI_value:cpi_value,Yahoo_value:yahoo_value,Nielsen_value:nielsen_value,Numbeo_value:numbeo_value" --values created_at:$(date +%s)
  elif [ -n "$(echo "$file" | grep "Cereal")" ]; then
    echo "Seeding cereal"
    kwil-cli database batch --path "$file" --action add_record --name cereal --map-inputs "Date:date_value,CPI_value:cpi_value,Yahoo_value:yahoo_value,Nielsen_value:nielsen_value,Numbeo_value:numbeo_value" --values created_at:$(date +%s)
  fi
done

rm -rf ./temp_csv
