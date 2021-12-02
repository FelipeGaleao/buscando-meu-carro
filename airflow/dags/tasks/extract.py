def extract_olx_veiculos(pagina):
    import pandas 
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
        df_propriedades_veiculos = df_propriedades_veiculos.reset_index().groupby('id').agg("first")
        df_propriedades_veiculos.to_csv('./raw/scrapping_olx_veiculos_ms.csv', mode='a', header=False, encoding='latin')
        
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
    carros_df.to_csv('./raw/scrapping_anuncios_shopcar.csv', mode='a', header=False, encoding='latin')

    # if(pagina > 2):
    #     carros_df.to_csv('./raw/scrapping_anuncios_shopcar.csv', mode='a', header=False, encoding='latin')
    # else:
    #     carros_df.to_csv('./raw/scrapping_anuncios_shopcar.csv', header=True,  encoding='latin')
