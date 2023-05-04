import requests
import pandas as pd

def search_spotify_podcasts(query):
    base_url = 'https://api.spotify.com/v1/search'
    params = {
        'q': query,
        'type': 'show',
        'market': 'BR',
        'limit': 50
    }
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
    response = requests.get(base_url, headers=headers, params=params)
    data = response.json()
    items = data.get('shows', {}).get('items', [])
    table = []
    print(f"Total items: {len(items)}")
    for item in items:
        if not item:
            continue
        name = item.get('name', '')
        description = item.get('description', '')
        podcast_id = item.get('id', '')
        total_episodes = item.get('total_episodes', '')
        table.append({
            'name': name,
            'description': description,
            'id': podcast_id,
            'total_episodes': total_episodes
        })
    df = pd.DataFrame(table, columns=['name', 'description', 'id', 'total_episodes'])
    df.to_csv('podcasts.csv', index=False)
    return df

# Exemplo de uso
podcast_df = search_spotify_podcasts('data hackers')
