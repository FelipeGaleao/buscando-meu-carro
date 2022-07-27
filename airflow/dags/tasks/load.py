def load_to_sql():
    import pandas as pd
    import pymssql
    import os
    import sqlalchemy
    
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

    # cnxn = pyodbc.connect(r'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
    #                       server+';DATABASE='+database+';UID='+username+';PWD=' + password + ';')

    engine = sqlalchemy.create_engine(f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)                    
    df.to_sql('mastertable', con=engine, if_exists='append', index=False)

    # cursor = cnxn.cursor()
    # # Insert Dataframe into SQL Server:
    # for index, row in df.iterrows():
    #     cursor.execute("""INSERT INTO dbo.mastertable ('Ano_Fabricacao',
    #             'Ano_Modelo',
    #             'Bairro_Anuncio',
    #             'Cidade',
    #             'Cidade_Anuncio',
    #             'Combustivel',
    #             'Cor',
    #             'Data_Extracao_Dados',
    #             'Endereco_Anuncio',
    #             'Estado_Anuncio',
    #             'Fonte',
    #             'KM',
    #             'Link',
    #             'Marca',
    #             'Modelo',
    #             'Preco',
    #             'Vendedor',
    #             'best_match_score',
    #             'gnv_kit',
    #             'id_marca',
    #             'id_modelo',
    #             'nome_marca',
    #             'nome_modelo',
    #             'uf') 
    #             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    #                    row.Ano_Fabricacao,
    #                    row.Ano_Modelo,
    #                    row.Bairro_Anuncio,
    #                    row.Cidade,
    #                    row.Cidade_Anuncio,
    #                    row.Combustivel,
    #                    row.Cor,
    #                    row.Data_Extracao_Dados,
    #                    row.Endereco_Anuncio,
    #                    row.Estado_Anuncio,
    #                    row.Fonte,
    #                    row.KM,
    #                    row.Link,
    #                    row.Marca,
    #                    row.Modelo,
    #                    row.Preco,
    #                    row.Vendedor,
    #                    row.best_match_score,
    #                    row.gnv_kit,
    #                    row.id_marca,
    #                    row.id_modelo,
    #                    row.nome_marca,
    #                    row.nome_modelo,
    #                    row.uf
    #                    )
    # cnxn.commit()
    # cursor.close()

    # Insert Dataframe into SQL Server:
