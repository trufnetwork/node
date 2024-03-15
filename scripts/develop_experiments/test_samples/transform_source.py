import os
import uuid

import pandas as pd

if __name__ == '__main__':
    # current script path
    path = os.path.dirname(os.path.realpath(__file__))
    files = os.listdir(path)
    # just csv
    files = [file for file in files if file.endswith('.csv')]

    transformed_path = os.path.join(path, 'transformed')

    # ensure the directory exists
    if not os.path.exists(transformed_path):
        os.makedirs(transformed_path)
    for file in files:
        df = pd.read_csv(f'{path}/{file}')
        # kuneiform does not support float, so we need to convert to integer
        df['value'] = df['value'] * 1000
        # convert to integer
        df['value'] = df['value'].astype(int)
        # add random uuid to ['id'] column
        df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df.to_csv(f'{transformed_path}/{file}', index=False)
