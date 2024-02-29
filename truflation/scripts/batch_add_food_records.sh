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
