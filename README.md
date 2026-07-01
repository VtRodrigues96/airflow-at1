################################  Como executar  #######################################
###### Pré-requisitos

Docker
Docker Compose
Python

###### Criar o caminho no prompt para a pasta da Atividade 1

Cd ~/airflow-at1 ---- Caminho para pasta
mkdir -p plugins ---- Pasta vazia para plugins
mkdir -p logs ---- Pasta vazia para logs

###### Subir o ambiente pelo prompt

docker compose up airflow-init ---- Inicializa o Airflow
Docker compose up -d ---- Inicializa todo escopo no Docker e cria acesso ao Airflow

####### Acessar o Airflow

http://localhost:8080
Usuário: admin
Senha: admin

####### Executar a DAG

Ative a DAG at1_pipeline e clique no Trigger DAG.

####### Após execução completa do DAG, você pode, se preferir, consultar o resultado no próprio prompt utilizando:
docker exec -it postgres_analytics psql -U airflow -d analytics

*Este código ativará o comando SQL no prompt

####### Aplicar o seguinte código para consulta no prompt:

SELECT
categoria,
quantidade_produtos,
preco_medio,
preco_minimo,
preco_maximo,
atualizado_em
FROM metricas_categoria
ORDER BY categoria;
