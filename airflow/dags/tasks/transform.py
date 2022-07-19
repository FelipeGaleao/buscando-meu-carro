
import pandas as pd
from fuzzymatcher import link_table, fuzzy_left_join
from fuzzywuzzy import process
from fuzzywuzzy import fuzz


def transform_mastertable():
    from datetime import date

    df_olx = pd.read_csv('./output_data/staging/anuncios_olx_fuzzy.csv', encoding='latin').drop(labels=['Unnamed: 0'], axis=1).rename(columns={'vendedor': 'Vendedor', 'bairro': 'Bairro_Anuncio',
                                                                                                                                            'cidade': 'Cidade_Anuncio', 'link_anuncio_olx': 'Link', 'regiao': 'Estado_Anuncio'})
    df_olx['Fonte'] = 'olx-v1'
    df_shopcar = pd.read_csv('./output_data/staging/anuncios_shopcar_fuzzy.csv',
                            encoding='latin').drop(labels=['Unnamed: 0'], axis=1)
    df_shopcar['Fonte'] = 'shopcar-v1'
    df_olx['Cidade'] = df_olx['Cidade_Anuncio']
    df_final = pd.concat([df_olx, df_shopcar])
    df_final['Marca'] = df_final['nome_marca']
    df_final['Data_Extracao_Dados'] =  date.today().strftime("%d/%m/%Y")
    df_final.to_csv('./output_data/trusted/mastertable-olx-shopcar.csv', mode='a+', encoding='latin', header=True)


def transform_olx_dataset():
    from glob import iglob

    path = r'./output_data/raw/olx_anuncios/*.csv'

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
    df_olx_veiculos['Preco'] = pd.to_numeric(
        df_olx_veiculos['Preco'], errors='coerce')
    df_olx_veiculos

    df_marca_modelo = pd.read_csv(
        './output_data/raw/scrapping_marcas_modelos_fipe.csv', encoding='latin').drop('Unnamed: 0', axis=1)
    df_fuzzy = fuzzy_left_join(
        df_olx_veiculos, df_marca_modelo, 'Modelo', 'nome_modelo')
    df_fuzzy = df_fuzzy.drop(labels=['__id_left', '__id_right'], axis=1)
    df_fuzzy.sort_values(by='best_match_score')
    df_fuzzy.to_csv(f'./output_data/staging/anuncios_olx_fuzzy.csv',
                    mode='w+', header=True, encoding='latin')
    df_fuzzy.head(10)


def transform_shopcar_dataset():
    import pandas as pd
    import numpy as np
    from datetime import date

    from fuzzywuzzy import fuzz
    from fuzzywuzzy import process
    from fuzzymatcher import link_table, fuzzy_left_join

    today = date.today()
    d1 = today.strftime("%d/%m/%Y")

    dataset = pd.read_csv(
        './output_data/raw/shopcar/scrapping_anuncios_shopcar.csv', encoding='latin')

    # geração de id do shop car
    dataset.columns = ['Unnamed: 0', 'Modelo', 'Ano', 'Cor',
                       'Combustivel', 'KM', 'Preco', 'Link', 'Vendedor', 'Cidade']

    dataset['Id_Anuncio_ShopCar'] = dataset['Link'].str.slice(start=-7)
    dataset.reset_index()
    dataset.set_index('Id_Anuncio_ShopCar', inplace=True)
    dataset.drop(labels=['Unnamed: 0'], axis=1)
    dataset = dataset[dataset.Preco != 'DETALHES']

    # correção do campo data ano/modelo

    ano_fabricacao = pd.Series(dataset['Ano']).str.slice(stop=2)
    dataset['Ano_Fabricacao'] = pd.to_numeric(ano_fabricacao, errors='coerce')
    dataset['Ano_Fabricacao'] = np.where(
        dataset['Ano_Fabricacao'] < 22, 2000 + dataset['Ano_Fabricacao'], 1900 + dataset['Ano_Fabricacao'])

    ano_modelo = pd.Series(dataset['Ano']).str.slice(start=3, stop=5)
    dataset['Ano_Modelo'] = pd.to_numeric(ano_modelo)
    dataset['Ano_Modelo'] = np.where(
        dataset['Ano_Modelo'] < 22, 2000 + dataset['Ano_Modelo'], 1900 + dataset['Ano_Modelo'])
    dataset = dataset.drop(labels=['Ano'], axis=1)
    dataset = dataset.drop(labels=['Unnamed: 0'], axis=1)

    # correção do campo KM
    dataset['KM'] = dataset['KM'].str.replace('Km', '')
    dataset['KM'] = dataset['KM'].str.replace('.', '')
    dataset['KM'] = pd.to_numeric(dataset['KM'], errors='ignore')

    # correção do campo PREÇO
    dataset['Preco'] = dataset['Preco'].str.replace('R', '')
    dataset['Preco'] = dataset['Preco'].str.replace('$', '')
    dataset['Preco'] = dataset['Preco'].str.replace('.', '')
    dataset['Preco'] = dataset['Preco'].str.replace(',00', '')
    dataset['Preco'] = pd.to_numeric(dataset['Preco'], errors='coerce')

    # endereços
    enderecos = pd.Series(dataset['Cidade'])
    dataset['Estado_Anuncio'] = enderecos.str.partition('/')[2]
    dataset['Endereco_Anuncio'] = enderecos.str.partition('-')[0]

    enderecos = enderecos.str.partition('-')[2]
    enderecos.str.partition(' - ')
    dataset['Bairro_Anuncio'] = enderecos.str.partition('-')[0]

    enderecos = enderecos.str.partition('-')[2]
    enderecos = enderecos.str.partition('/')
    dataset['Cidade_Anuncio'] = enderecos[0]

    # marca
    marca = dataset['Link'].str.partition('/')[2]
    dataset['Marca'] = marca.str.partition(
        '/')[2].str.partition('/')[2].str.partition('/')[2].str.partition('/')[0]
    dataset['Data_Extracao_Dados'] = d1

    df_anuncios = dataset
    df_anuncios.to_csv(f'./output_data/staging/anuncios_shopcar2.csv',
                       header=True, encoding='latin')

    df_marca_modelo = pd.read_csv(
        './output_data/raw/scrapping_marcas_modelos_fipe.csv', encoding='latin')
    df_fuzzy = fuzzy_left_join(
        df_anuncios, df_marca_modelo, 'Modelo', 'nome_modelo')
    df_fuzzy = df_fuzzy.drop(labels=['__id_left', '__id_right'], axis=1)
    df_fuzzy.sort_values(by='best_match_score')
    df_fuzzy.to_csv(f'./output_data/staging/anuncios_shopcar_fuzzy.csv',
                    mode='w+', header=True, encoding='latin')
