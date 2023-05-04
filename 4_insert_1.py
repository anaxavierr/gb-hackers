# Importando as bibliotecas necessárias
import requests
import pandas as pd

# Definindo a função de busca de podcasts do Spotify
def get_datahackers_episodes():

    # ID do podcast DataHackers no Spotify
    podcast_id = '1oMIHOXsrLFENAeM743g93' 

    # URL da API do Spotify para obter informações dos episódios do podcast
    episodes_url = f"https://api.spotify.com/v1/shows/{podcast_id}/episodes"

    # Credenciais de acesso à API do Spotify
    client_id = '58f1d9a3438d4148b3835418d78c780f'
    client_secret = '665ae28bb8f74c28bc6b349ae0e966cb'

    # URL para autenticação e obtenção do token de acesso à API do Spotify
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_response = requests.post(auth_url, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })

    if auth_response.status_code == 200:
        access_token = auth_response.json()['access_token']
    else:
        print(f"Authentication error with status code: {auth_response.status_code}")
        return pd.DataFrame(), pd.DataFrame()

    # Headers para a requisição à API do Spotify com o token de acesso
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    all_episodes = []
    limit = 50
    offset = 0
    total = 1

    # Loop para obter as informações de todos os episódios
    while offset < total:

        # Parâmetros da requisição à API do Spotify
        params = {
            'market': 'BR',
            'limit': limit,
            'offset': offset
        }

        # Requisição à API do Spotify para obter informações dos episódios
        # Realiza uma requisição HTTP GET à URL que contém informações dos episódios do podcast,
        # passando os parâmetros necessários e o token de autorização no cabeçalho da requisição.
        response = requests.get(episodes_url, headers=headers, params=params)

        if response.status_code == 200:
            # Caso a resposta da requisição seja bem-sucedida (código HTTP 200),
            # converte o conteúdo da resposta (que está em formato JSON) para um dicionário Python.
            data = response.json()
            # Extrai a lista de episódios (que está dentro da chave 'items' do dicionário) do conteúdo da resposta.
            episodes = data.get('items', [])
            # Itera sobre a lista de episódios e extrai as informações relevantes de cada um
            # (nome, descrição, id, data de lançamento e duração em milissegundos), adicionando-as
            # à lista de todos os episódios.
            for episode in episodes:
                name = episode.get('name', '')
                description = episode.get('description', '')
                episode_id = episode.get('id', '')
                release_date = episode.get('release_date', '')
                duration_ms = episode.get('duration_ms', '')
                language = episode.get('language', '')
                explicit = episode.get('explicit', '')
                type_ = episode.get('type', '')
                all_episodes.append({
                    'id': episode_id,
                    'name': name,
                    'description': description,
                    'release_date': release_date,
                    'duration_ms': duration_ms,
                    'language': language,
                    'explicit': explicit,
                    'type': type
                })
            # Atualiza o valor de 'total' para a quantidade total de episódios disponíveis,
            # e incrementa o valor de 'offset' para obter a próxima página de resultados.
            total = data.get('total', 0)
            offset += limit
        else:
            # Caso ocorra algum erro na requisição, imprime uma mensagem de erro com o código de status da resposta.
            print(f"Request error with status code: {response.status_code}")
            # Retorna dois DataFrames vazios para indicar que a operação não foi bem-sucedida.
            return pd.DataFrame(), pd.DataFrame()

    # Converte a lista de todos os episódios para um DataFrame Pandas com as colunas especificadas.
    df_all_episodes = pd.DataFrame(all_episodes, columns=['id', 'name', 'description', 'release_date', 'duration_ms', 'language', 'explicit', 'type'])
    # Salva o DataFrame em um arquivo CSV.
    df_all_episodes.to_csv('datahackers_all_episodes.csv', index=False)

    # Filtra o DataFrame para selecionar apenas os episódios que contêm a string 'Grupo Boticário' na descrição.
    df_filtered = df_all_episodes[df_all_episodes['description'].str.contains('Grupo Boticário', case=False)]
    # Salva o DataFrame filtrado em um arquivo CSV.
    df_filtered.to_csv('datahackers_episodes_groupo_boticario.csv', index=False)

    # Retorna os dois DataFrames: o DataFrame com todos os episódios e o DataFrame filtrado.
    return df_all_episodes, df_filtered

# Uso da função
df_all_episodes, df_groupo_boticario = get_datahackers_episodes()
