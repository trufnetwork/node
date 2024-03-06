import os
import re

import pandas as pd

if __name__ == '__main__':
    file_to_read = 'categories-tables-us.csv'

    dirname = os.path.dirname(__file__)
    full_path = os.path.join(dirname, file_to_read)

    # import ./categories-tables-us.csv
    # read:
    # - source_id as text
    # - category_id as text
    # - subcategory_id as text
    # - table as text
    # - relative_importance as float
    # - category as text
    # - subcategory as text
    categories = pd.read_csv(full_path,
                             dtype={'source_id': 'str', 'category_id': 'str', 'subcategory_id': 'str', 'table': 'str',
                                    'relative_importance': 'float', 'category': 'str', 'subcategory': 'str'})
    # let's create a categories_only table that contains just rows that doens't contain data in the subcategory column
    categories_only = categories[categories['subcategory'].isna()]

    categories_only = categories_only[['category', 'relative_importance', 'category_id']]

    # let's rename category_id to id, category to name
    categories_only = categories_only.rename(columns={'category_id': 'id', 'category': 'name'})

    # let's add a parent_id column with 999 as value
    categories_only['parent_id'] = "999"

    # let's create a subcategories_only table that contains just rows that contain data in the subcategory column, and nothing on the table column
    subcategories_only = categories[categories['subcategory'].notna() & categories['table'].isna()]

    subcategories_only = subcategories_only[
        ['subcategory', 'relative_importance', 'category_id', 'subcategory_id']]

    # let's rename category_id to parent_id, subcategory_id to id, subcategory to name
    subcategories_only = subcategories_only.rename(
        columns={'category_id': 'parent_id', 'subcategory_id': 'id', 'subcategory': 'name'})

    # let's create a tables_only table that contains just rows that contain data in the table column

    tables_only = categories[categories['table'].notna()]

    tables_only = tables_only[['relative_importance', 'subcategory_id', 'table']]

    # let's rename subcategory_id to parent_id, table to name, and duplicate table as id too
    tables_only = tables_only.rename(
        columns={'subcategory_id': 'parent_id', 'table': 'name'})

    tables_only['id'] = tables_only['name']

    # let's check if all tables produced here contains the same 4 columns: id, name, parent_id, relative_importance
    expected_columns = ['id', 'name', 'parent_id', 'relative_importance']

    # let's reorder the columns to make sure they are in the same order
    categories_only = categories_only[expected_columns]
    subcategories_only = subcategories_only[expected_columns]
    tables_only = tables_only[expected_columns]

    assert categories_only.columns.tolist() == expected_columns
    assert subcategories_only.columns.tolist() == expected_columns
    assert tables_only.columns.tolist() == expected_columns

    # create a single dataframe with all tables
    all_tables = pd.concat([categories_only, subcategories_only, tables_only])

    # find maximum decimal places used by any relative_importance
    maximum_decimal_places = all_tables['relative_importance'].apply(lambda x: len(str(x).split('.')[1])).max()

    # multiply every relative importance by maximum_decimal_places and convert to int
    all_tables['relative_importance'] = (all_tables['relative_importance'] * (10 ** maximum_decimal_places)).astype(int)

    all_tables = pd.concat(
        [pd.DataFrame([{'id': '999', 'name': 'CPI', 'parent_id': None, 'relative_importance': 0}]), all_tables])

    # sort dataframe by parent_id
    all_tables = all_tables.sort_values(by='parent_id')

    # add is_primitive column with True if id is not in parent_id column
    all_tables['is_primitive'] = ~all_tables['id'].isin(all_tables['parent_id'])


    # reprocess all names to be database_name friendly:
    # Vehicle purchases (net outlay) -> vehicle_purchases_net_outlay
    # Residential phone service, VOIP, and phone cards -> residential_phone_service_voip_and_phone_cards
    # Alcohol & Tobacco -> alcohol_and_tobacco

    def name_to_database_name(name):
        name = name.lower()
        name = re.sub(r'[^a-z0-9\-]+', '_', name)
        name = re.sub(r'_$', '', name)
        return name


    all_tables['source_database_name'] = all_tables['name'].apply(name_to_database_name)


    # we need to make sure any database_name don't have more than 32 characters.
    # if some of them has, we need to delete one character alternatively from the end of the name, until it has 32 characters or less
    # we make it to be partially readable, and try to remain unique

    def adjust_name_length(name):
        # Check if the name is longer than 32 characters
        prefix = name[:1]
        sufix = name[1:]
        while len(prefix + sufix) > 32:
            # Delete one character alternatively, from the start
            # i.e. a + bcdef -> ac + def -> ace + f

            prefix += sufix[1:2]
            sufix = sufix[2:]

        return prefix + sufix


    def fix_kwil_db_name(name):
        name = adjust_name_length(name)
        # should remove invalid characters, replacing by _
        name = re.sub(r'[^a-z0-9_]+', '_', name)

        return name


    all_tables['database_name'] = all_tables['source_database_name'].apply(fix_kwil_db_name)

    # remove from source_database_name values when it's not a primitive, it doesn't make sense
    all_tables['source_database_name'] = all_tables['source_database_name'].where(all_tables['is_primitive'], None)

    # add parent_database_name column
    all_tables['parent_database_name'] = all_tables['parent_id'].apply(
        lambda x: all_tables[all_tables['id'] == x]['database_name'].values[0] if x is not None else None)

    # We don't want to have created duplicated database names that maps to different tables

    # first get a table that has unique source_database_name
    unique_source_database_name = all_tables.drop_duplicates(subset='source_database_name')

    # then get a table that has unique database_name
    unique_database_name = all_tables.drop_duplicates(subset='database_name')

    # now try to find for if for a database_name, there are more than one source_database_name in unique_source_database_name
    for database_name in unique_database_name['database_name']:
        source_database_names = \
            unique_source_database_name[unique_source_database_name['database_name'] == database_name][
                'source_database_name']
        if len(source_database_names) > 1:
            print(f'duplicated database_name: {database_name}')
            print(source_database_names)

            # make it error out
            assert False
    # save all_tables to a csv file
    all_tables.to_csv(os.path.join(dirname, 'all_tables.csv'), index=False)
    composed_streams = all_tables.rename(
        columns={'parent_database_name': 'parent_stream', 'database_name': 'stream', 'relative_importance': 'weight'})

    composed_streams = composed_streams[['parent_stream', 'stream', 'weight']]

    # save to ../../composed_streams.csv
    composed_streams.to_csv(os.path.join(dirname, '../../composed_streams.csv'), index=False)
