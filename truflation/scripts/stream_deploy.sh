# drop the existing databases
kwil-cli database drop meats_cpi
kwil-cli database drop meats_yahoo
kwil-cli database drop meats_nielsen
kwil-cli database drop meats_numbeo

kwil-cli database drop cereal_cpi
kwil-cli database drop cereal_yahoo
kwil-cli database drop cereal_nielsen
kwil-cli database drop cereal_numbeo

kwil-cli database drop dairy_cpi
kwil-cli database drop dairy_yahoo
kwil-cli database drop dairy_nielsen
kwil-cli database drop dairy_numbeo

kwil-cli database drop fruits_cpi
kwil-cli database drop fruits_yahoo
kwil-cli database drop fruits_nielsen
kwil-cli database drop fruits_numbeo

kwil-cli database drop other_cpi
kwil-cli database drop other_yahoo
kwil-cli database drop other_nielsen
kwil-cli database drop other_numbeo

kwil-cli database drop food_away_from_home_cpi
kwil-cli database drop food_away_from_home_yahoo
kwil-cli database drop food_away_from_home_numbeo
kwil-cli database drop food_away_from_home_bigmac

kwil-cli database drop meats
kwil-cli database drop cereal
kwil-cli database drop dairy
kwil-cli database drop fruits
kwil-cli database drop other
kwil-cli database drop food_away_from_home

kwil-cli database drop food_at_home

kwil-cli database drop food_and_beverages


# wait for 10 seconds to make sure the databases are dropped on kwil node
sleep 10

# deploy the databases
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=meats_cpi
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=meats_yahoo
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=meats_nielsen
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=meats_numbeo

kwil-cli database deploy -p=../base_schema/base_schema.kf -n=cereal_cpi
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=cereal_yahoo
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=cereal_nielsen
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=cereal_numbeo

kwil-cli database deploy -p=../base_schema/base_schema.kf -n=dairy_cpi
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=dairy_yahoo
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=dairy_nielsen
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=dairy_numbeo

kwil-cli database deploy -p=../base_schema/base_schema.kf -n=fruits_cpi
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=fruits_yahoo
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=fruits_nielsen
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=fruits_numbeo

kwil-cli database deploy -p=../base_schema/base_schema.kf -n=other_cpi
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=other_yahoo
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=other_nielsen
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=other_numbeo

kwil-cli database deploy -p=../base_schema/base_schema.kf -n=food_away_from_home_cpi
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=food_away_from_home_yahoo
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=food_away_from_home_numbeo
kwil-cli database deploy -p=../base_schema/base_schema.kf -n=food_away_from_home_bigmac
