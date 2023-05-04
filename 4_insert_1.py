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

    if auth_response.status_code == 200:
        access_token = auth_response.json()['access_token']
    else:
        print(f"Authentication error with status code: {auth_response.status_code}")
        return pd.DataFrame(), pd.DataFrame()

    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    all_episodes = []
    limit = 50
    offset = 0
    total = 1
    while offset < total:
        params = {
            'market': 'BR',
            'limit': limit,
            'offset': offset
        }
        response = requests.get(episodes_url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            episodes = data.get('items', [])
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
            total = data.get('total', 0)
            offset += limit
        else:
            print(f"Request error with status code: {response.status_code}")
            return pd.DataFrame(), pd.DataFrame()

    df_all_episodes = pd.DataFrame(all_episodes, columns=['id', 'name', 'description', 'release_date', 'duration_ms', 'language', 'explicit', 'type'])
    df_all_episodes.to_csv('datahackers_all_episodes.csv', index=False)

    df_filtered = df_all_episodes[df_all_episodes['description'].str.contains('Grupo Boticário', case=False)]
    df_filtered.to_csv('datahackers_episodes_groupo_boticario.csv', index=False)

    return df_all_episodes, df_filtered

# Exemplo de uso
df_all_episodes, df_groupo_boticario = get_datahackers_episodes()
