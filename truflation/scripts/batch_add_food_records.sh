# create temp_csv folder
rm -rf ./temp_csv
mkdir -p ./temp_csv

# for each file on ./raw_csv, create temp file that have cleaned data
# The header will be id, date_value, value for each file
# for each column, multiply the value by 1000
# save the file to ./temp_csv
# seperate cpi, yahoo, nielsen, numbeo into different files
# example meat_cpi.csv, meat_yahoo.csv, meat_nielsen.csv, meat_numbeo.csv
for file in ./raw_csv/*.csv; do
  echo "Processing $file"
  if echo "$file" | grep -q "Away"; then
    awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, $2*1000} {close("uuidgen")}' "$file" > ./temp_csv/$(basename "$file" .csv)_cpi.csv
    awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, $3*1000} {close("uuidgen")}' "$file" > ./temp_csv/$(basename "$file" .csv)_yahoo.csv
    awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, $4*1000} {close("uuidgen")}' "$file" > ./temp_csv/$(basename "$file" .csv)_numbeo.csv
    awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, $5*1000} {close("uuidgen")}' "$file" > ./temp_csv/$(basename "$file" .csv)_bigmac.csv
  else
    awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, $2*1000} {close("uuidgen")}' "$file" > ./temp_csv/$(basename "$file" .csv)_cpi.csv
    awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, $3*1000} {close("uuidgen")}' "$file" > ./temp_csv/$(basename "$file" .csv)_yahoo.csv
    awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, $4*1000} {close("uuidgen")}' "$file" > ./temp_csv/$(basename "$file" .csv)_nielsen.csv
    awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, $5*1000} {close("uuidgen")}' "$file" > ./temp_csv/$(basename "$file" .csv)_numbeo.csv
  fi
done

# seed the database
for file in ./temp_csv/*.csv; do
  echo "Processing file: $file"
  if echo "$file" | grep -q "Cereal" && echo "$file" | grep -q "cpi"; then
    kwil-cli database batch --path "$file" --action add_record --name cereal_cpi --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Cereal" && echo "$file" | grep -q "nielsen"; then
    kwil-cli database batch --path "$file" --action add_record --name cereal_nielsen --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Cereal" && echo "$file" | grep -q "numbeo"; then
    kwil-cli database batch --path "$file" --action add_record --name cereal_numbeo --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Cereal" && echo "$file" | grep -q "yahoo"; then
    kwil-cli database batch --path "$file" --action add_record --name cereal_yahoo --values created_at:$(date +%s)

  elif echo "$file" | grep -q "Dairy" && echo "$file" | grep -q "cpi"; then
    kwil-cli database batch --path "$file" --action add_record --name dairy_cpi --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Dairy" && echo "$file" | grep -q "nielsen"; then
    kwil-cli database batch --path "$file" --action add_record --name dairy_nielsen --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Dairy" && echo "$file" | grep -q "numbeo"; then
    kwil-cli database batch --path "$file" --action add_record --name dairy_numbeo --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Dairy" && echo "$file" | grep -q "yahoo"; then
    kwil-cli database batch --path "$file" --action add_record --name dairy_yahoo --values created_at:$(date +%s)

  elif echo "$file" | grep -q "Fruits" && echo "$file" | grep -q "cpi"; then
    kwil-cli database batch --path "$file" --action add_record --name fruits_cpi --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Fruits" && echo "$file" | grep -q "nielsen"; then
    kwil-cli database batch --path "$file" --action add_record --name fruits_nielsen --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Fruits" && echo "$file" | grep -q "numbeo"; then
    kwil-cli database batch --path "$file" --action add_record --name fruits_numbeo --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Fruits" && echo "$file" | grep -q "yahoo"; then
    kwil-cli database batch --path "$file" --action add_record --name fruits_yahoo --values created_at:$(date +%s)

  elif echo "$file" | grep -q "Meats" && echo "$file" | grep -q "cpi"; then
    kwil-cli database batch --path "$file" --action add_record --name meats_cpi --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Meats" && echo "$file" | grep -q "nielsen"; then
    kwil-cli database batch --path "$file" --action add_record --name meats_nielsen --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Meats" && echo "$file" | grep -q "numbeo"; then
    kwil-cli database batch --path "$file" --action add_record --name meats_numbeo --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Meats" && echo "$file" | grep -q "yahoo"; then
    kwil-cli database batch --path "$file" --action add_record --name meats_yahoo --values created_at:$(date +%s)

  elif echo "$file" | grep -q "Other" && echo "$file" | grep -q "cpi"; then
    kwil-cli database batch --path "$file" --action add_record --name other_cpi --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Other" && echo "$file" | grep -q "nielsen"; then
    kwil-cli database batch --path "$file" --action add_record --name other_nielsen --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Other" && echo "$file" | grep -q "numbeo"; then
    kwil-cli database batch --path "$file" --action add_record --name other_numbeo --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Other" && echo "$file" | grep -q "yahoo"; then
    kwil-cli database batch --path "$file" --action add_record --name other_yahoo --values created_at:$(date +%s)

  elif echo "$file" | grep -q "Away" && echo "$file" | grep -q "cpi"; then
    kwil-cli database batch --path "$file" --action add_record --name food_away_from_home_cpi --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Away" && echo "$file" | grep -q "numbeo"; then
    kwil-cli database batch --path "$file" --action add_record --name food_away_from_home_numbeo --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Away" && echo "$file" | grep -q "yahoo"; then
    kwil-cli database batch --path "$file" --action add_record --name food_away_from_home_yahoo --values created_at:$(date +%s)
  elif echo "$file" | grep -q "Away" && echo "$file" | grep -q "bigmac"; then
    kwil-cli database batch --path "$file" --action add_record --name food_away_from_home_bigmac --values created_at:$(date +%s)
  else
    echo "No match for $file"
  fi
done

rm -rf ./temp_csv
