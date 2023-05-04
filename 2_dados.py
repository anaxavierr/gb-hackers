import requests
import pandas as pd

def get_datahackers_episodes():
    podcast_id = '1oMIHOXsrLFENAeM743g93'
    episodes_url = f"https://api.spotify.com/v1/shows/{podcast_id}/episodes"
    client_id = '58f1d9a3438d4148b3835418d78c780f'
    client_secret = '665ae28bb8f74c28bc6b349ae0e966cb'
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_response = requests.post(auth_url, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    access_token = auth_response.json()['access_token']
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    all_episodes = []
    limit = 50
    offset = 0
    total = 1
    while offset < total:
        params = {
            'type': 'episode',
            'market': 'BR',
            'limit': limit,
            'offset': offset
        }
        response = requests.get(episodes_url, headers=headers, params=params)
        data = response.json()
        episodes = data.get('items', [])
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
        total = data.get('total', 0)
        offset += limit
    df = pd.DataFrame(all_episodes, columns=['name', 'description', 'id', 'release_date', 'duration_ms'])
    df.to_csv('datahackers_episodes.csv', index=False)
    print(f"Total de episódios encontrados: {len(all_episodes)}")
    return df

# Exemplo de uso
datahackers_episodes_df = get_datahackers_episodes()
