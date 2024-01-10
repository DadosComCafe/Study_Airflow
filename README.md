# Study Airflow
    Repositório de estudo de algumas das principais ferramentas disponíveis com algumas das principais ferramentas disponíveis no airflow.

## How to Run
    Primeiramente, é necessário que você tenha o airflow rodando on-premise, ou seja na sua máquina. Então segue aqui um tutorial de como fazer isto com este repositório.

* __Obs__:
    - É necessário ter o docker instalado em sua máquina;
    - É necessário ter uma conta na google cloud platform;
    - É necessário ter uma conta no kaggle.

* __Clone o repositório:__
    - git clone https://github.com/DadosComCafe/Study_Airflow

* __Build dos containers do airflow:__
    - docker compose up --airflow-init `para buildar as imagens e fazer os registros necessários no postgres que está servindo o airflow`
    - docker compose up `ao final da linha acima, execute esta linha. Não é mais necessário executar o comando com a flag, somente no primeiro momento.`

* __Acessar a interface do airflow:__
    - Ao final dos processos do docker, acesse: http://localhost:8080/;
    - Para acessar a interface, é necessário realizar o login com as credenciais do banco de dados postgres do airflow, definidas no docker-compose.yaml. Por padrão airflow:airflow
        - ![Login com as credenciais do postgres](/images/login_airflow.png "Login para primeiro acesso.")

* __Carregar as variávies através da interface do airflow:__
    - Você deve utilizar o arquivo example_variables.json para criar um json com suas credenciais de acesso. Uma vez criado este arquivo, faça o upload dele conforme as imagens:
        - ![Envio do arquivo json](/images/acessando_variables.png "Enviando json para o airflow")
        - ![Arquivo enviado](/images/tela_de_variaveis.png "Verificando as variávies")

* __Criar um banco de dados postgres:__
    - Você precisa criar um schema de nome kaggle_dag_airflow;
        - ![Acessar o banco](/images/acessando_banco_com_beekeeper.png "Usando as credenciais do postgres")
    - Então acesse o banco e rode: CREATE DATABASE kaggle_dag_airflow;
        - ![Criar o banco de dados](/images/criando_banco_de_dados.png "Executando o comando para criar o banco de dados")

* __Inserir registro de dataset e dataset owner do kaggle no mongodb:__
    - Como há uma task que vai tentar baixar o dataset a partir de um registro no mongo, é necessário que haja este registro:
    - Portanto, crie um database no mongodb, e dentro deste database, execute o seguinte comando:

    `db.kaggle_datasets.insertOne({
    "dataset_owner": "joebeachcapital",
    "dataset_name": "fast-food",
    "downloaded": false
})`
    - Como sugere a imagem:
        - ![Criar o banco de dados](/images/criando_database_no_mongodb.png "Executando o comando para inserir o registro")

* __Inicialize a dag e deixe rolar:__
    - Agora basta inicializar a dag, e acompanhar todo o processo na cloud, bancos de dados e bigquery;
        - ![Inicializa a dag](/images/inicializa_dag.png "Inicializando a dag")


* __Rodar os testes:__
    - make test `Isto irá executar os testes que as tasks possuem`
