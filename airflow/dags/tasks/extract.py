
import requests 
import pandas as pd

def extract_webmotors_page(**args):
    req_veiculos = requests.get("https://www.webmotors.com.br/api/search/car?url=https://www.webmotors.com.br/carros%2Fms%3Fanunciante%3DConcession%25C3%25A1ria%257CLoja%26estadocidade%3DMato%2520Grosso%2520do%2520Sul", params="actualPage=" + str(index))
    req_veiculos = json.loads(req_veiculos.text)
    try:
        req_veiculos = req_veiculos['SearchResults']
        json_str = json.dumps(req_veiculos)
        veiculos = pandas.read_json(json_str)
        tabela_especificacoes = pandas.json_normalize(veiculos['Specification'])
        veiculos = veiculos.join(other= tabela_especificacoes)

        tabela_precos = pandas.json_normalize(veiculos['Prices'])
        veiculos = veiculos.join(other= tabela_precos)
        
        tabela_vendedor = pandas.json_normalize(veiculos['Seller'])
        veiculos = veiculos.join(other= tabela_vendedor)
        dados_veiculos = pandas.concat([dados_veiculos, veiculos])
    except:
        print(f"Ocorreu um erro na página {index}")
        
    dados_veiculos = dados_veiculos.drop(labels=['Prices', 'Channels', 'VehicleAttributes', 'Media', 'Specification', 'Seller'], axis=1)
    dados_veiculos.to_csv('output/anuncios_webmotors.csv')