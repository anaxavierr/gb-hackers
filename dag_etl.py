import datetime
import pandas as pd
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from airflow.operators.python import PythonOperator
import requests

# Define os argumentos padrões do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 5, 4),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    dag_id='spotify_podcasts',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Define a função de busca de podcasts do Spotify
def search_spotify_podcasts(query):
    # Definindo a URL base e os parâmetros da API do Spotify
    base_url = 'https://api.spotify.com/v1/search'
    params = {
        'q': query,
        'type': 'show',
        'market': 'BR',
        'limit': 50
}

    # Define o nome do arquivo CSV
    csv_filename = 'podcasts.csv'

    # Define as credenciais de autenticação da API do Spotify
    client_id = '58f1d9a3438d4148b3835418d78c780f'
    client_secret = '665ae28bb8f74c28bc6b349ae0e966cb'

    # Realizando a autenticação e obtendo o token de acesso
    auth_url = 'https://accounts.spotify.com/api/token'
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

        # Adicionando os dados do podcast à lista
        table.append({
            'name': name,
            'description': description,
            'id': podcast_id,
            'total_episodes': total_episodes
        })

    # Convertendo a lista em um DataFrame do Pandas
    df = pd.DataFrame(table, columns=['name', 'description', 'id', 'total_episodes'])
    
    # Exportando o DataFrame para um arquivo CSV
    df.to_csv('podcasts.csv', index=False)

    # Convertendo o DataFrame para um objeto JSON serializável
    json_data = df.to_json(orient='records')

    # Exportando o DataFrame para um arquivo CSV
    df.to_csv(csv_filename, index=False)

    # Retornando o DataFrame com os dados dos podcasts
    return json_data

# Uso da função de busca de podcasts do Spotify
podcast_df = search_spotify_podcasts('data hackers')


search_spotify_task = PythonOperator(
    task_id='search_spotify_podcasts',
    dag=dag,
    python_callable=search_spotify_podcasts,
    op_kwargs={'query': 'data hackers'}
)





# Define a função para buscar os episódios do podcast DataHackers no Spotify
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
                    'type': type_
                })

        # Atualiza os valores de offset e total para buscar a próxima página de resultados (caso exista)
        offset += limit
        total = data.get('total', 0)


    # Converte a lista de episódios para um DataFrame do pandas e retorna os resultados
    df_all_episodes = pd.DataFrame(all_episodes, columns=['id', 'name', 'description', 'release_date', 'duration_ms', 'language', 'explicit', 'type'])

    # Converte a lista de todos os episódios para um DataFrame Pandas com as colunas especificadas.
    df_all_episodes = pd.DataFrame(all_episodes, columns=['id', 'name', 'description', 'release_date', 'duration_ms', 'language', 'explicit', 'type'])
    # Salva o DataFrame em um arquivo CSV.
    df_all_episodes.to_csv('datahackers_all_episodes.csv', index=False)

    # Filtra o DataFrame para selecionar apenas os episódios que contêm a string 'Grupo Boticário' na descrição.
    df_filtered = df_all_episodes[df_all_episodes['description'].str.contains('Grupo Boticário', case=False)]
    # Salva o DataFrame filtrado em um arquivo CSV.
    df_filtered.to_csv('datahackers_episodes_groupo_boticario.csv', index=False)
    df_filtered = df_all_episodes[df_all_episodes['description'].str.contains('Grupo Boticário', case=False)]
    return df_all_episodes, df_filtered


# Uso da função
df_all_episodes, df_grupo_boticario = get_datahackers_episodes()


get_datahackers_task = PythonOperator(
    task_id='get_datahackers_episodes',
    dag=dag,
    python_callable=get_datahackers_episodes,
)


create_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset',
    dag=dag,
    dataset_id='spotify_podcasts',
    project_id='your_project_id'
)


create_table_task = BigQueryCreateEmptyTableOperator(
    task_id='create_table',
    dag=dag,
    table_id='podcasts',
    dataset_id='spotify_podcasts',
    project_id='your_project_id',
    schema_fields=[
        {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'description', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'total_episodes', 'type': 'INTEGER', 'mode': 'REQUIRED'}
    ]
)





def insert_data(**kwargs):
    # Função para inserir os dados obtidos na API do Spotify no BigQuery

    # Obtendo o DataFrame com os dados dos podcasts
    df = kwargs['task_instance'].xcom_pull(task_ids='search_spotify_podcasts')

    if df.empty:
        return

    # Transformando os dados do DataFrame para o formato adequado
    df['total_episodes'] = df['total_episodes'].astype('int64')

    # Preparando os dados para a inserção no BigQuery
    rows = df.to_dict(orient='records')

    # Executando a inserção no BigQuery
    insert_query = f"""
        INSERT INTO `spotify_podcasts.podcasts` (name, description, id, total_episodes)
        VALUES (@name, @description, @id, @total_episodes)
    """
    insert_job = BigQueryInsertJobOperator(
        task_id='insert_data',
        sql=insert_query,
    parameters=rows,
    bigquery_conn_id='bigquery_default',
    use_legacy_sql=False,
    dag=dag,
)
    insert_job.execute(context=kwargs)

insert_data_task = PythonOperator(
task_id='insert_data',
dag=dag,
python_callable=insert_data,
provide_context=True,
)

search_spotify_task >> get_datahackers_task >> create_dataset_task >> create_table_task >> insert_data_task

