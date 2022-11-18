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
        '../airflow/output_data/trusted/mastertable-olx-shopcar.csv', encoding='latin')
    df.columns = df.columns.str.replace(' ', '')
    columns = ['Ano_Fabricacao',
               'Ano_Modelo',
               'Bairro_Anuncio',
               'Cidade',
               'Cidade_Anuncio',
               'Combustivel',
               'Cor',
               'Data_Extracao_Dados',
               'Endereco_Anuncio',
               'Estado_Anuncio',
               'Fonte',
               'KM',
               'Link',
               'Marca',
               'Modelo',
               'Preco',
               'Vendedor',
               'anuncio_id_olx',
               'best_match_score',
               'gnv_kit',
               'id_marca',
               'id_modelo',
               'nome_marca',
               'nome_modelo',
               'uf'
               ]
    df = df[columns]
    df.columns = df.columns.str.strip()


    # create engine mysql sql alchemy
    engine = sqlalchemy.create_engine(
        f"mysql+pymysql://{username}:{password}@{server}/{database}")           
    df.to_sql('mastertable', con=engine, if_exists='replace', index=False, chunksize=5000)
