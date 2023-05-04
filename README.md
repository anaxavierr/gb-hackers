# gb-hackers
Case Grupo Boticário no Data Hackers

Instruções:

item 1 (arquitetura_gb.pdf):
					
Cenário Atual
					
Estamos passando por um processo de transformação digital, onde o seu papel será definir a arquitetura de referência para plataforma de dados do Grupo Boticário e ser uma referência técnica para engenheiros e analistas de dados. No cenário atual, utilizamos SAP Hana como nosso repositório principal de data warehouse. Existem processos de ETL que fazem ingestão de dados de 50 transacionais. Mais de 90% das bases são de origem transacionais de diferentes DBMS’s (DB2, MS SQL etc) e estão alocados em ambiente on-premises. Além do SAP Hana, a empresa possui algumas aplicações hospedadas em nuvens públicas como Microsoft Azure e Amazon Web Services. Dentro da empresa, o tratamento e o consumo dos dados são tratados em silos, onde diferentes unidades de negócios acabam utilizando diferentes ferramentas para processar, analisar dados e apresentar dados. Algumas ferramentas que podemos citar como exemplo são Jupyter Notebook, Qlick, Qlick Sense. Outro aspecto importante está ligado a governança de dados, onde aspectos como acesso a dados sensíveis, catalogação e permisionamento carecem de melhorias.
					
O que esperamos?
					
1. Que você defina uma arquitetura de referência com tecnologias de alguma nuvem pública, preferencialmente AWS ou GCP. Você deve considerar os seguintes requisitos:
 • Permear as camadas de ingestão, processamento, armazenamento, consumo, análise, segurança e governança;
					
• Substituição gradativa do cenário on-premises atual;
• Incorporação de componentes e tecnologias que permitam a analisarmos dados em tempo real;
• Que a arquitetura considere componentes que a habilitem a empresa organizar e fornecer dados para diferentes fins, tais como: Analytics, Data Science, API’s e serviços para integrações com aplicações. Ressaltando que necessariamente precisaremos manter a comunicação on-premises x cloud para diversas finalidades.



item 2 (etl_dag):

1. Realizar a importação dos dados dos 3 arquivos em uma tabela criada por você no banco de dados de sua escolha;
					
2. Com os dados importados, modelar 4 novas tabelas e implementar processos que façam as transformações necessárias e insiram as seguintes visões nas tabelas:
					
a. Tabela 1: Consolidado de vendas por ano e mês;
b. Tabela 2: Consolidado de vendas por marca e linha;
c. Tabela 3: Consolidado de vendas por marca, ano e mês;
d. Tabela 4: Consolidado de vendas por linha, ano e mês;



item 3:

Passo 1 - (1_request.py)
Criar um método para realizar uma pesquisa no Spotify via requests e trazer os primeiros 50 resultados referente a podcasts procurando pelo termo “data hackers” e criar uma tabela apenas com os campos abaixo:
					
name = Nome do poscast.
description = Descrição sobre o programa de poscast.
id = Identificador único do programa. total_episodes = Total de episódios lançados até o momento.
					
Passo 2 - (2_dados.py)
Realizar a extração de dados de todos os episódios lançados pelos Data Hackers via requests.

Passo 3 - (3_insert.py)
Ingerir resultado do Passo 2 em duas tabelas seguindo os critérios abaixo:
					
a. Resultado de todos os episódios.
					
b. Apenas com os resultados dos episódios com participação do Grupo Boticário.
				
			
Passo 4 - (4_insert_1.py)
Levar apenas os campos abaixo para as tabelas a e b do Passo 3:
id - Identificação do episódio.
name - Nome do episódio.
description - Descrição do episódio.
release_date - Data de lançamento do episódio.
duration_ms - Duração em milissegundos do episódio. language - Idioma do episódio.
explicit - Flag booleano se o episódio possui conteúdo explícito. type - O tipo de faixa de áudio (Ex: música / programa)

