def load_to_sql():
    import pandas as pd
    import os
    import sqlalchemy
    import pymysql
    server = os.environ['EXTERNAL_DB_HOSTNAME']
    database = os.environ['EXTERNAL_DB_DBNAME']
    username = os.environ['EXTERNAL_DB_USER']
    password = os.environ['EXTERNAL_DB_PASSWORD']

    import pyodbc
    import pandas as pd

    df = pd.read_csv(
        './output_data/trusted/mastertable-olx-shopcar.csv', encoding='utf-8')
    df.columns = df.columns.str.replace(" ", "")
    df.columns = df.columns.str.replace('\n       ', '')
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lstrip()

    # create engine mysql sql alchemy
    engine = sqlalchemy.create_engine(
        f"mysql+pymysql://{username}:{password}@{server}/{database}")           
    df.to_sql('mastertable', con=engine, if_exists='replace', index=False, chunksize=5000)
