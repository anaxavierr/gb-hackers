# gb-hackers
Case Grupo Boticário no Data Hackers

Instruções:

Passo 1 -
Criar um método para realizar uma pesquisa no Spotify via requests e trazer os primeiros 50 resultados referente a podcasts procurando pelo termo “data hackers” e criar uma tabela apenas com os campos abaixo:
					
name = Nome do poscast.
description = Descrição sobre o programa de poscast.
id = Identificador único do programa. total_episodes = Total de episódios lançados até o momento.
					
Passo 2 - 
Realizar a extração de dados de todos os episódios lançados pelos Data Hackers via requests.

Passo 3 -
Ingerir resultado do Passo 2 em duas tabelas seguindo os critérios abaixo:
					
a. Resultado de todos os episódios.
					
b. Apenas com os resultados dos episódios com participação do Grupo Boticário.
				
			
Passo 4 -
Levar apenas os campos abaixo para as tabelas a e b do Passo 3:
id - Identificação do episódio.
name - Nome do episódio.
description - Descrição do episódio.
release_date - Data de lançamento do episódio.
duration_ms - Duração em milissegundos do episódio. language - Idioma do episódio.
explicit - Flag booleano se o episódio possui conteúdo explícito. type - O tipo de faixa de áudio (Ex: música / programa)

