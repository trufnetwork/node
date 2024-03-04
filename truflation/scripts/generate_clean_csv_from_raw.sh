# create temp_csv folder
rm -rf ./temp_csv
mkdir -p ./temp_csv

# for each file on ./raw_csv, create temp file that have cleaned data
# The header will be id, date_value, value for each file
# for each column, multiply the value by 1000
# save the file to ./temp_csv
# separate cpi, yahoo, nielsen, numbeo into different files
# example: meats_cpi.csv, meats_yahoo.csv, meats_nielsen.csv, meats_numbeo.csv
for file in ./food_and_beverages_raw_csv/*; do
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
