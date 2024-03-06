import json
import os

import pandas as pd

"""

 This script is intended to help pulling data from DB directly for development purposes
 it's necessary to have mysqlclient and pandas available to run it
 also remember to configure the secret_db_credentials.json file with the correct credentials

 But if don't fulfill these requirements, don't worry, as the result should be pushed to the repository
 and the data will be available at ./raw_from_db

 It is also limited to 200 rows per table, ordered by `date` with maximum date of 2024-02-28 for sample purposes


 The result of this script will be a set of csv files at ./raw_from_db
 with the following format:

 | date       | value                            | created_at         |
 |------------|----------------------------------|---------------------|
 | 2024-02-28 | 0.12345 (precision is variable) | 2024-02-28 00:00:00 |

 the name of each file will be the same as the table name, for example: com_numbeo_us_mortgage_interest.csv
"""

if __name__ == '__main__':
    # if __file__ is not defined, get the current working directory
    current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()

    # get user from file ./secret_db_credentials.json
    with open(os.path.join(current_dir, 'secret_db_credentials.json')) as f:
        db_credentials = json.load(f)

    db_user = db_credentials['user']
    db_password = db_credentials['password']
    db_host = db_credentials['host']
    db_schema = db_credentials['schema']

    # get ./produce_source_maps/all_tables.csv file
    # filter data by is_primitive = True
    # get the `id` column

    all_tables = pd.read_csv(os.path.join(current_dir, 'produce_source_maps', 'all_tables.csv'))
    all_tables = all_tables[all_tables['is_primitive'] == True]

    # deduplicate the list by database_name
    all_tables = all_tables.drop_duplicates(subset=['database_name'])

    # why there's new and source?
    # database_name will have a limitation of max 32 characters to deploy to KWIL
    database_names = all_tables['database_name'].tolist()
    source_database_names = all_tables['source_database_name'].tolist()

    skip_tables = [
        "com_truflation_yahoo_food"  # this doesn't exist
    ]

    for idx, source_database_name in enumerate(source_database_names):
        new_database_name = database_names[idx]

        if source_database_name in skip_tables:
            print(f"Skipping {source_database_name}...")
            continue

        # check if already exist
        if os.path.exists(os.path.join(current_dir, 'raw_from_db', f"{new_database_name}.csv")):
            print(f"Data from {source_database_name} already exists at ./raw_from_db/{new_database_name}.csv")
            continue

        print(f"Pulling data from {source_database_name}...")

        # get 200 rows, ordered by `date` in descending order, with maximum date of 2024-02-28
        stmt = f"SELECT * FROM `{db_schema}`.`{source_database_name}` WHERE date <= '2024-02-28' ORDER BY date DESC LIMIT 200"  # execute the statement using mysql
        df = pd.read_sql(stmt, 'mysql://' + db_user + ':' + db_password + '@' + db_host + '/' + db_schema)

        # make sure the column order is `date`, `value`, `created_at`
        df = df[['date', 'value', 'created_at']]

        # save csv file at ./raw_primitives/<table>.csv
        df.to_csv(os.path.join(current_dir, 'raw_from_db', f"{new_database_name}.csv"), index=False)
        print(f"Data from {source_database_name} saved to ./raw_from_db/{new_database_name}.csv")
