# deploy the composed database
kwil-cli database deploy -p=../food_and_beverages_schemas/composed_food_cereal_1.kf  -n=cereal
kwil-cli database deploy -p=../food_and_beverages_schemas/composed_food_meats_1.kf  -n=meats
kwil-cli database deploy -p=../food_and_beverages_schemas/composed_food_dairy_1.kf  -n=dairy
kwil-cli database deploy -p=../food_and_beverages_schemas/composed_food_fruits_1.kf  -n=fruits
kwil-cli database deploy -p=../food_and_beverages_schemas/composed_food_other_1.kf  -n=other
kwil-cli database deploy -p=../food_and_beverages_schemas/composed_food_away_from_home_1.kf  -n=food_away_from_home
