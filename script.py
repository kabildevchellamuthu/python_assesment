import pandas as pd
from sqlalchemy import create_engine

# create an database connection with sqlalchemy
engine = create_engine('postgresql+psycopg2://postgres:Qm7pn8mJZ007@localhost/assesment')

def process_data(path):
    # Specify the batch size to read from CSV file and INSERT in SQL
    # This batch size depends on the SQL server performance
    chunksize = 10 ** 3
    with pd.read_csv(path, chunksize=chunksize) as reader:
        for chunk in reader:
            # chunksize will act as a batch size 
            # method="multi" will insert that batch i single shot
            # if_exists='append' will update data in same database
            completed = chunk.to_sql('test_od', engine, chunksize=chunksize, method='multi', if_exists='append')
            print('Process completed', completed)
    return

path = 'data/data_process.csv'

if __name__ == '__main__':
    process_data(path)