from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import os 
import sys 

## Import tasks folder
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
sys.path.append('/opt/airflow/dags')
# from tasks import extract

dag_id = 'CMC-extract-data-from-portals'

default_args = {
    'owner': 'CMC @felipegaleao',
    'start_date': datetime(2021, 10, 24),
    "email": ["maycon.mota@ufms.br"],
    'description': 'Dag to extract info from Shopcar and Webmotors',
    "email_on_failure": True,
    "email_on_retry": False,
    'retries': 3,
    'catchup': False
}


dag = DAG(
    dag_id = dag_id,
    default_args = default_args,
    max_active_runs=5,
    concurrency = 64,
    schedule_interval="0 2 * * *"
)





## Tasks => TODO: Resolver o problema do diretório de tasks para deixar o arquivo principal da dag menos poluído


# def extract_webmotors_page(index):
#     import requests
#     import pandas as pd 
#     import json 
#     headers = {
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
#     }
#     req_veiculos = requests.get("https://www.webmotors.com.br/api/search/car?url=https://www.webmotors.com.br/carros%2Fms%3Fanunciante%3DConcession%25C3%25A1ria%257CLoja%26estadocidade%3DMato%2520Grosso%2520do%2520Sul", params="actualPage=" + str(index), headers=headers)
#     req_veiculos = json.loads(req_veiculos.text)
#     dados_veiculos = pandas.DataFrame()
#     try:
#         req_veiculos = req_veiculos['SearchResults']
#         json_str = json.dumps(req_veiculos)
#         veiculos = pandas.read_json(json_str)
#         tabela_especificacoes = pandas.json_normalize(veiculos['Specification'])
#         veiculos = veiculos.join(other= tabela_especificacoes)

#         tabela_precos = pandas.json_normalize(veiculos['Prices'])
#         veiculos = veiculos.join(other= tabela_precos)
        
#         tabela_vendedor = pandas.json_normalize(veiculos['Seller'])
#         veiculos = veiculos.join(other= tabela_vendedor)
#         dados_veiculos = pandas.concat([dados_veiculos, veiculos])
#     except:
#         print(f"Ocorreu um erro na página {index}")
        
#     dados_veiculos = dados_veiculos.drop(labels=['Prices', 'Channels', 'VehicleAttributes', 'Media', 'Specification', 'Seller'], axis=1)
#     if(index == 1):
#        dados_veiculos.to_csv('raw/anuncios_webmotors.csv')
#     else:
#        dados_veiculos.to_csv('raw/anuncios_webmotors.csv', mode='a', headers='false')

def extract_shopcar_page(pagina):
    import pandas 
    import json 
    import requests
    from bs4 import BeautifulSoup
    
    print(f"Iniciando scrapping ...... {pagina}")
    url = f"https://www.shopcar.com.br/busca.php?tipo=1&marca=&string=&ordenar=valor_asc&pagina={pagina}"
    res = requests.get(url)
    soup = BeautifulSoup(res.text)
    index = 0
    carro_dados = []
    
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


    carro_dados.append([carro_modelo, anoModelo_anoFabricacao, corCarro, combustivelCarro, kmCarro, preco, link_veiculo, vendedor, cidade])
    carros_df = pandas.DataFrame(carro_dados)
    carros_df.columns = ['Modelo', 'Ano', 'Cor',' Combustível','KM', 'Preco', 'Link', 'Vendedor', 'Cidade']
    carros_df.head(10)
    if(pagina > 2):
        carros_df.to_csv('./raw/scrapping_anuncios_shopcar.csv', mode='a', header=False)
    else:
        carros_df.to_csv('./raw/scrapping_anuncios_shopcar.csv', header=True)

for i in range(1, 593):
    # scrapping_webmotors = PythonOperator(
    #             task_id="EXTRACT_DATA_WEBMOTORS_PAGE_{}".format(i),
    #             python_callable = extract_webmotors_page,
    #             op_kwargs = {
    #                 "index" : i,
    #             },
    #             dag=dag)
    
    scrapping_shopcar_page = PythonOperator(
                task_id="EXTRACT_DATA_SHOPCAR_PAGE_{}".format(i),
                python_callable = extract_shopcar_page,
                op_kwargs = {
                    "pagina" : i,
                },
                dag=dag)