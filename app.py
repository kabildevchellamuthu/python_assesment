from flask import Flask, request
import os
import csv
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import time

app = Flask(__name__)
# create an database connection with sqlalchemy
engine = create_engine('postgresql+psycopg2://postgres:Qm7pn8mJZ007@localhost/assesment')

# POST method to receive a file and write into the database with new table named with current date and time
@app.route("/api/file-import", methods=['POST'])
def post_method():
    try:
        file = request.files['files']
        filename = file.filename
        file.save(os.path.join('', filename))
        
        now = datetime.now()
        date_str = now.strftime("%Y_%m_%d_%H_%M_%S")
        # CSV to Pandas dataframe
        df = pd.read_csv(filename)

        # construct table name with file name and current datetime
        table_name = f"{filename.replace('.csv', '')}_{date_str}"

        #  Write data with pandas dataframe to SQL
        df.to_sql(table_name, engine, chunksize=100, method='multi')
    except Exception as e:
        return {'status_code': 1, 'message':'success'}
    return {'status_code': 0, 'message':'success'}