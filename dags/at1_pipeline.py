from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook


def callback_sucesso(context):
    print(
        f"SUCESSO: {context['task_instance'].task_id}"
    )


def callback_retry(context):
    print(
        f"RETRY: {context['task_instance'].task_id}"
    )


def callback_falha(context):
    print(
        f"FALHA: {context['task_instance'].task_id}"
    )


@dag(
    dag_id="at1_pipeline",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(
        2025,
        1,
        1,
        tz="America/Sao_Paulo"
    ),
    catchup=False,
    tags=["at1"],
)
def at1_pipeline():

    with TaskGroup(group_id="ingestao"):

        @task(
            retries=3,
            retry_delay=timedelta(seconds=10),
            retry_exponential_backoff=True,
            on_success_callback=callback_sucesso,
            on_retry_callback=callback_retry,
            on_failure_callback=callback_falha,
        )
        def buscar_produtos():

            print("Buscando produtos da FakeStore API...")

            try:

                response = requests.get(
                    "https://fakestoreapi.com/products",
                    timeout=30
                )

                response.raise_for_status()

                produtos = response.json()

                print(
                    f"Produtos encontrados: {len(produtos)}"
                )

                return produtos

            except Exception as e:

                print(
                    f"Erro ao consultar API: {e}"
                )

                raise

        @task
        def extrair_categorias(produtos):

            print("Extraindo categorias...")

            categorias = list(
                {produto["category"] for produto in produtos}
            )

            print(
                f"Categorias encontradas: {categorias}"
            )

            return categorias

        produtos = buscar_produtos()

        categorias = extrair_categorias(produtos)

    with TaskGroup(group_id="analise"):

        @task(
            pool="ecommerce_pool"
        )
        def processar_categoria(categoria, produtos):

            print(
                f"Processando categoria: {categoria}"
            )

            produtos_categoria = [
                produto
                for produto in produtos
                if produto["category"] == categoria
            ]

            precos = [
                produto["price"]
                for produto in produtos_categoria
            ]

            quantidade_produtos = len(
                produtos_categoria
            )

            preco_medio = round(
                sum(precos) / quantidade_produtos,
                2
            )

            preco_minimo = min(precos)

            preco_maximo = max(precos)

            resultado = {
                "categoria": categoria,
                "quantidade_produtos": quantidade_produtos,
                "preco_medio": preco_medio,
                "preco_minimo": preco_minimo,
                "preco_maximo": preco_maximo,
            }

            print(resultado)

            return resultado

        @task
        def consolidar_metricas(resultados):

            print("Consolidando métricas...")
            print(resultados)

            return resultados

        @task
        def salvar_postgres(metricas):

            print("Salvando no PostgreSQL...")

            hook = PostgresHook(
                postgres_conn_id="postgres_analytics"
            )

            # Cria a tabela caso não exista
            hook.run("""
            CREATE TABLE IF NOT EXISTS metricas_categoria (
                categoria VARCHAR(255) PRIMARY KEY,
                quantidade_produtos INTEGER NOT NULL,
                preco_medio NUMERIC(10,2),
                preco_minimo NUMERIC(10,2),
                preco_maximo NUMERIC(10,2),
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            sql = """
            INSERT INTO metricas_categoria (
                categoria,
                quantidade_produtos,
                preco_medio,
                preco_minimo,
                preco_maximo
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s
            )
            ON CONFLICT (categoria)
            DO UPDATE SET
                quantidade_produtos = EXCLUDED.quantidade_produtos,
                preco_medio = EXCLUDED.preco_medio,
                preco_minimo = EXCLUDED.preco_minimo,
                preco_maximo = EXCLUDED.preco_maximo,
                atualizado_em = CURRENT_TIMESTAMP
            """

            for metrica in metricas:

                hook.run(
                    sql,
                    parameters=(
                        metrica["categoria"],
                        metrica["quantidade_produtos"],
                        metrica["preco_medio"],
                        metrica["preco_minimo"],
                        metrica["preco_maximo"],
                    ),
                )

            print(f"{len(metricas)} registros gravados")

        categorias_processadas = processar_categoria.partial(
            produtos=produtos
        ).expand(
            categoria=categorias
        )

        metricas = consolidar_metricas(
            categorias_processadas
        )

        salvar_postgres(metricas)


at1_pipeline()