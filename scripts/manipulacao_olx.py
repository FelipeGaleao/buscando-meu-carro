from glob import iglob
import pandas as pd

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from fuzzymatcher import link_table, fuzzy_left_join

path = r'./../airflow/raw/olx_anuncios/*.csv'

all_rec = iglob(path, recursive=True)
dataframes = (pd.read_csv(f, encoding='latin') for f in all_rec)
big_dataframe = pd.concat(dataframes, ignore_index=True)


df_olx_veiculos = big_dataframe.copy()
df_olx_veiculos = df_olx_veiculos.drop(labels=['id', 'index', 'car_features', 'cartype', 'category', 'doors',
                                               'end_tag', 'exchange', 'owner', 'motorpower', 'gearbox', 'car_steering', 'preco_anterior', 'financial', 'carcolor',
                                               'fuel'], axis=1)
df_olx_veiculos = df_olx_veiculos.rename(columns={'mileage': 'KM', 'regdate': 'Ano_Fabricacao',
                                                  'vehicle_brand': 'Marca', 'vehicle_model': 'Modelo', 'preco_anuncio': 'Preco', 'anunciante': 'vendedor'})
df_olx_veiculos['Preco'] = df_olx_veiculos['Preco'].str.replace('R', '')
df_olx_veiculos['Preco'] = df_olx_veiculos['Preco'].str.replace('$', '')
df_olx_veiculos['Preco'] = df_olx_veiculos['Preco'].str.replace('.', '')
df_olx_veiculos['Preco'] = df_olx_veiculos['Preco'].str.replace(',00', '')
df_olx_veiculos['Preco'] = pd.to_numeric(df_olx_veiculos['Preco'], errors='coerce')
df_olx_veiculos


df_marca_modelo = pd.read_csv(
    '../airflow/raw/scrapping_marcas_modelos_fipe.csv', encoding='latin').drop('Unnamed: 0', axis=1)
df_fuzzy = fuzzy_left_join(df_olx_veiculos, df_marca_modelo, 'Modelo', 'nome_modelo')
df_fuzzy = df_fuzzy.drop(labels=['__id_left', '__id_right'], axis=1)
df_fuzzy.sort_values(by='best_match_score')
df_fuzzy.to_csv(f'../airflow/staging/anuncios_olx_fuzzy.csv',
                mode='a', header=False, encoding='latin')



