# drop the existing databases
kwil-cli database drop meat
kwil-cli database drop cereal
kwil-cli database drop food_at_home

# wait for 5 seconds to make sure the databases are dropped on kwil node
sleep 5

# deploy the databases
kwil-cli database deploy -p=../food_and_beverages_schemas/foodstream.kf -n=meat
kwil-cli database deploy -p=../food_and_beverages_schemas/foodstream.kf -n=cereal
