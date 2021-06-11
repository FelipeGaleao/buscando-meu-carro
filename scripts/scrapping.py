# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from bs4 import BeautifulSoup
import requests
import csv
import pandas


# %%
carro_dados = []
for pagina in range(562):
    print(f"Iniciando scrapping ...... {pagina}")
    url = f"https://www.shopcar.com.br/busca.php?tipo=1&marca=&string=&ordenar=valor_asc&pagina={pagina}"
    res = requests.get(url)
    soup = BeautifulSoup(res.text)
    index = 0
    for carro in soup.find_all(class_='destaque-lista'):
        carro_modelo = carro('li')[1]('div')[0].get_text()
        anoModelo_anoFabricacao = carro('li')[1]('div')[2].get_text()
        corCarro = carro('li')[1]('div')[3].get_text()
        combustivelCarro = carro('li')[1]('div')[4].get_text()
        preco = carro(class_='preco')[0].get_text()
        try:
            kmCarro = carro(class_='caract-km')[0].get_text()
        except:
            kmCarro = 'Não disponível'
        link_veiculo = carro('a')[1].get('href')
        url = link_veiculo
        res = requests.get(url)
        soup = BeautifulSoup(res.text)
        vendedor = soup.find_all(class_='dados1')[0].find_all(class_='nome')[0].get_text()
        cidade = soup.find_all(class_='dados1')[0].find_all(class_='endereco')[0].get_text()
        index += 1
        carro_dados.append([carro_modelo, anoModelo_anoFabricacao, corCarro, combustivelCarro, kmCarro, preco, link_veiculo, vendedor, cidade])
        print(f"Já foram incluídos {index} carros")

    for carro in soup.find_all(class_='itens'):
        carro_modelo = carro('li')[1]('div')[0].get_text()
        anoModelo_anoFabricacao = carro('li')[1]('div')[2].get_text()
        corCarro = carro('li')[1]('div')[3].get_text()
        combustivelCarro = carro('li')[1]('div')[4].get_text()
        preco = carro(class_='preco')[0].get_text()
        try:
            kmCarro = carro(class_='caract-km')[0].get_text()
        except:
            kmCarro = 'Não disponível'
        link_veiculo = carro('a')[1].get('href')
        url = link_veiculo
        res = requests.get(url)
        soup = BeautifulSoup(res.text)
        vendedor = soup.find_all(class_='dados1')[0].find_all(class_='nome')[0].get_text()
        cidade = soup.find_all(class_='dados1')[0].find_all(class_='endereco')[0].get_text()
        index += 1
        carro_dados.append([carro_modelo, anoModelo_anoFabricacao, corCarro, combustivelCarro, kmCarro, preco, link_veiculo, vendedor, cidade])
        
        print(f"Já foram incluídos {index} carros")
        


# %%
carros_df = pandas.DataFrame(carro_dados)
carros_df.columns = ['Modelo', 'Ano', 'Cor',' Combustível','KM', 'Preco', 'Link', 'Vendedor', 'Cidade']
carros_df.to_csv('output/scrapping_nuncios_shopcar.csv')


# %%



