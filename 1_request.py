# Importando as bibliotecas necessárias
import requests
import pandas as pd

# Definindo a função de busca de podcasts do Spotify
def search_spotify_podcasts(query):
    # Definindo a URL base e os parâmetros da API do Spotify
    base_url = 'https://api.spotify.com/v1/search'
    params = {
        'q': query,
        'type': 'show',
        'market': 'BR',
        'limit': 50
    }

    # Definindo as credenciais de autenticação da API do Spotify
    client_id = '58f1d9a3438d4148b3835418d78c780f'
    client_secret = '665ae28bb8f74c28bc6b349ae0e966cb'
    auth_url = 'https://accounts.spotify.com/api/token'

    # Realizando a autenticação e obtendo o token de acesso
    auth_response = requests.post(auth_url, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    access_token = auth_response.json()['access_token']

    # Definindo o cabeçalho de autenticação para as requisições à API do Spotify
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    # Realizando a requisição à API do Spotify e obtendo os resultados
    response = requests.get(base_url, headers=headers, params=params)
    data = response.json()
    items = data.get('shows', {}).get('items', [])

    # Criando uma lista para armazenar os dados de cada podcast e iterando sobre os resultados
    table = []
    print(f"Total items: {len(items)}")
    for item in items:
        if not item:
            continue

        # Obtendo os dados de cada podcast
        name = item.get('name', '')
        description = item.get('description', '')
        podcast_id = item.get('id', '')
        total_episodes = item.get('total_episodes', '')

        # Armazenando os dados de cada podcast na lista
        table.append({
            'name': name,
            'description': description,
            'id': podcast_id,
            'total_episodes': total_episodes
        })

    # Convertendo a lista de dados em um DataFrame do Pandas e salvando como um arquivo CSV
    df = pd.DataFrame(table, columns=['name', 'description', 'id', 'total_episodes'])
    df.to_csv('podcasts.csv', index=False)

    # Retornando o DataFrame com os dados dos podcasts
    return df

# Uso da função de busca de podcasts do Spotify
podcast_df = search_spotify_podcasts('data hackers')
