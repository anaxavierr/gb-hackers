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
    access_token = auth_response.json()['access_token']

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
            'type': 'episode',
            'market': 'BR',
            'limit': limit,
            'offset': offset
        }

        # Requisição à API do Spotify para obter informações dos episódios
        response = requests.get(episodes_url, headers=headers, params=params)
        data = response.json()
        episodes = data.get('items', [])

        # Loop para obter as informações de cada episódio obtido na requisição
        for episode in episodes:
            name = episode.get('name', '')
            description = episode.get('description', '')
            episode_id = episode.get('id', '')
            release_date = episode.get('release_date', '')
            duration_ms = episode.get('duration_ms', '')
            all_episodes.append({
                'name': name,
                'description': description,
                'id': episode_id,
                'release_date': release_date,
                'duration_ms': duration_ms
            })

        # Atualização do total de episódios e do offset para a próxima requisição
        total = data.get('total', 0)
        offset += limit

    # Criação de um dataframe com as informações de todos os episódios
    df = pd.DataFrame(all_episodes, columns=['name', 'description', 'id', 'release_date', 'duration_ms'])

    # Exportação do dataframe para um arquivo csv
    df.to_csv('datahackers_episodes.csv', index=False)

    # Exibição do número total de episódios encontrados
    print(f"Total de episódios encontrados: {len(all_episodes)}")
    return df

# Uso da função
datahackers_episodes_df = get_datahackers_episodes()
