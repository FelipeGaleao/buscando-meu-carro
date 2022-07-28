def extract_marcas_modelo_fipe():
    import pandas as pd 
    import numpy as np 
    from datetime import date
    import requests
    import json

    def buscarMarca():
        url = "https://veiculos.fipe.org.br/api/veiculos//ConsultarMarcas?codigoTabelaReferencia=267&codigoTipoVeiculo=1"

        payload = "{\r\n    \r\n}"
        headers = {
        'Content-Type': 'text/plain',
        'Cookie': 'ROUTEID=.3'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        return pd.DataFrame(json.loads(response.content))

    def buscarCarros(marcas):
        modelos = pd.DataFrame()
        for marca in marcas:
            url = f"https://veiculos.fipe.org.br/api/veiculos//ConsultarModelos?codigoTipoVeiculo=1&codigoTabelaReferencia=267&codigoMarca={marca}"
            payload = "{\r\n    \r\n}"
            headers = {
            'Content-Type': 'text/plain',
            'Cookie': 'ROUTEID=.3'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            try:
                veiculos = json.loads(response.content)
            except:
                print('')
            df_temp = pd.DataFrame(veiculos['Modelos'])
            df_temp['id_marca'] = f"{marca}"
            modelos = pd.concat([df_temp, modelos])
        return modelos

    marcas_df = buscarMarca()
    marcas_df.columns = ['nome_marca', 'id_marca']
    marcas_df

    modelos_df = buscarCarros(marcas_df['id_marca'])

    df = pd.DataFrame(modelos_df)
    df.columns = ['nome_modelo', 'id_modelo', 'id_marca']
    df
    dataset_final = marcas_df.merge(modelos_df, how='inner')
    dataset_final.to_csv('./output_data/raw/scrapping_marcas_modelos_fipe.csv', encoding='latin')

def extract_olx_veiculos(pagina):
    import pandas as pd
    import json 
    import requests
    from bs4 import BeautifulSoup
    url = f"https://ms.olx.com.br/autos-e-pecas/carros-vans-e-utilitarios?o={pagina}&re=39"
    request_headers = {
        'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/50.0.2661.102 '
        'Safari/537.36 '
    }
    res = requests.get(url, headers=request_headers)
    soup = BeautifulSoup(res.text)
    anuncios = soup.find(id='ad-list')
    link_anuncio = anuncios.find_all("a")
    for anuncio in link_anuncio:
        res = requests.get(
            anuncio['href'], headers=request_headers, stream=True)
        soup = BeautifulSoup(res.content, features="html.parser")
        data = soup.find(id="initial-data")['data-json']
        data = json.loads(data)
        data = data['ad']
        propriedades = data['properties']
        df_propriedades_veiculos = pd.DataFrame(data=propriedades)
        df_propriedades_veiculos = pd.pivot(
            df_propriedades_veiculos, columns='name', values='value')
        df_propriedades_veiculos['id'] = 1
        df_propriedades_veiculos['anuncio_id_olx'] = data['listId']
        df_propriedades_veiculos['link_anuncio_olx'] = anuncio['href']
        df_propriedades_veiculos['preco_anuncio'] = data['priceValue']
        df_propriedades_veiculos['anunciante'] = data['user']['name']
        df_propriedades_veiculos['preco_anterior'] = data['oldPrice']
        df_propriedades_veiculos['bairro'] = data['location']['neighbourhood']
        df_propriedades_veiculos['cidade'] = data['location']['municipality']
        df_propriedades_veiculos['uf'] = data['location']['uf']
        df_propriedades_veiculos['regiao'] = data['location']['region']
        df_propriedades_veiculos = df_propriedades_veiculos.reset_index().groupby('id').agg("first")
        df_propriedades_veiculos.to_csv(f'./output_data/raw/olx_anuncios/{data["listId"]}.csv', mode='w+', header=True, encoding='latin')
        
def extract_shopcar_page(pagina):
    import pandas 
    import json 
    import requests
    from bs4 import BeautifulSoup
    import cloudscraper
    from selenium import webdriver
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service as ChromeService
    from webdriver_manager.chrome import ChromeDriverManager
    from fake_useragent import UserAgent

    ua = UserAgent()
    userAgent = ua.random
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument(f'user-agent={userAgent}')
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument('--disable-dev-shm-usage')        

    print(f"Iniciando scrapping ...... {pagina}")
    url = f"https://www.shopcar.com.br/busca.php?tipo=1&marca=&string=&ordenar=valor_asc&pagina={pagina}"
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), chrome_options=chrome_options)

    driver.get(url)

    res = driver.page_source
   # print(res)

    soup = BeautifulSoup(res, "html.parser")
    index = 0
    carro_dados = []

    for carro in soup.find_all(class_='itens'):
        carro_detalhes = carro.select(".coluna-2")
        try:
            carro_modelo = carro_detalhes[0]('div')[0].get_text()
            anoModelo_anoFabricacao = carro_detalhes[0]('div')[2].get_text()
            corCarro = carro_detalhes[0]('div')[3].get_text()
            combustivelCarro = carro_detalhes[0]('div')[4].get_text()
            preco = carro_detalhes[0]('div')[7].get_text()
            try:
                kmCarro = carro[0]('div')[5].get_text()
            except:
                kmCarro = 'Não disponível'
            link_veiculo = carro('a')[1].get('href')
            url = link_veiculo
            driver.get(url)
            res = driver.page_source
            soup = BeautifulSoup(res)
            vendedor = soup.find_all(class_='dados1')[0].find_all(class_='nome')[0].get_text()
            cidade = soup.find_all(class_='dados1')[0].find_all(class_='endereco')[0].get_text()
            carro_dados.append([carro_modelo, anoModelo_anoFabricacao, corCarro, combustivelCarro, kmCarro, preco, link_veiculo, vendedor, cidade])
        except:
            pass
        
    carros_df = pandas.DataFrame(carro_dados)
    print(carro_dados)
    carros_df.columns = ['Modelo', 'Ano', 'Cor',' Combustível','KM', 'Preco', 'Link', 'Vendedor', 'Cidade']
    carros_df.to_csv('./output_data/raw/shopcar/scrapping_anuncios_shopcar.csv', mode='a', header=False, encoding='latin')

    # if(pagina > 2):
    #     carros_df.to_csv('./raw/scrapping_anuncios_shopcar.csv', mode='a', header=False, encoding='latin')
    # else:
    #     carros_df.to_csv('./raw/scrapping_anuncios_shopcar.csv', header=True,  encoding='latin')
