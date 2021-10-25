def transform_shopcar_dataset():
    import pandas as pd 
    import numpy as np 
    from datetime import date
    
    today = date.today()
    d1 = today.strftime("%d/%m/%Y")

    dataset = pd.read_csv('./raw/scrapping_anuncios_shopcar.csv')

    ## geração de id do shop car

    dataset['Id_Anuncio_ShopCar'] = pd.Series(dataset['Link']).str.slice(start=-7)
    dataset.reset_index()
    dataset.set_index('Id_Anuncio_ShopCar', inplace=True)
    dataset.drop(labels=['Unnamed: 0'], axis=1)

    ## correção do campo data ano/modelo

    ano_fabricacao = pd.Series(dataset['Ano']).str.slice(stop=2)
    dataset['Ano_Fabricacao'] = pd.to_numeric(ano_fabricacao)
    dataset['Ano_Fabricacao'] = np.where(dataset['Ano_Fabricacao'] < 22, 2000 + dataset['Ano_Fabricacao'], 1900 + dataset['Ano_Fabricacao'])

    ano_modelo = pd.Series(dataset['Ano']).str.slice(start=3, stop=5)
    dataset['Ano_Modelo'] = pd.to_numeric(ano_modelo)
    dataset['Ano_Modelo'] = np.where(dataset['Ano_Modelo'] < 22, 2000 + dataset['Ano_Modelo'], 1900 + dataset['Ano_Modelo'])
    dataset = dataset.drop(labels=['Ano'], axis=1)
    dataset = dataset.drop(labels=['Unnamed: 0'], axis=1)

    ## correção do campo KM
    dataset['KM'] = dataset['KM'].str.replace('Km', '')
    dataset['KM'] = dataset['KM'].str.replace('.', '')
    dataset['KM'] = pd.to_numeric(dataset['KM'], errors='coerce')

    ## correção do campo KM
    dataset['Preco'] = dataset['Preco'].str.replace('R', '')
    dataset['Preco'] = dataset['Preco'].str.replace('$', '')
    dataset['Preco'] = dataset['Preco'].str.replace('.', '')
    dataset['Preco'] = dataset['Preco'].str.replace(',00', '')
    dataset['Preco'] = pd.to_numeric(dataset['Preco'], errors='coerce')

    ## endereços 
    enderecos = pd.Series(dataset['Cidade'])
    dataset['Estado_Anuncio'] = enderecos.str.partition('/')[2]
    dataset['Endereco_Anuncio'] = enderecos.str.partition('-')[0]

    enderecos = enderecos.str.partition('-')[2]
    enderecos.str.partition(' - ')
    dataset['Bairro_Anuncio'] = enderecos.str.partition('-')[0]

    enderecos = enderecos.str.partition('-')[2]
    enderecos = enderecos.str.partition('/')
    dataset['Cidade_Anuncio'] = enderecos[0]

    ## marca 
    marca = dataset['Link'].str.partition('/')[2]
    dataset['Marca'] = marca.str.partition('/')[2].str.partition('/')[2].str.partition('/')[2].str.partition('/')[0]
    dataset['Data_Extracao_Dados'] = d1

    dataset.to_csv(f'./staging/anuncios_shopcar.csv', mode='a', header=False)
