# Projeto - Pipeline de Dados com Apache Airflow

## 🚀 Inicialização do Ambiente

### 1. Inicializar o Airflow

Execute o comando abaixo para realizar a configuração inicial do ambiente Airflow e crie as pastas de plugins e logs antes da execução:

```bash
mkdir -p logs plugins
chmod -R 777 logs
docker compose up airflow-init
```

### 2. Subir todos os serviços

Após a inicialização, execute:

```bash
docker compose up -d
```

Este comando iniciará todos os containers definidos no ambiente Docker, incluindo o Apache Airflow e os demais serviços necessários para a execução do pipeline.

---

## 🌐 Acesso ao Airflow

Após a inicialização dos serviços, acesse a interface web do Airflow:

**URL:** http://localhost:8080

### Credenciais de acesso

| Campo   | Valor |
| ------- | ----- |
| Usuário | admin |
| Senha   | admin |

---

## ▶️ Execução da DAG

1. Acesse a interface do Airflow.
2. Localize a DAG **`at1_pipeline`**.
3. Ative a DAG utilizando o botão de habilitação.
4. Clique em **Trigger DAG** para iniciar a execução.

Aguarde a conclusão de todas as tarefas antes de prosseguir para a etapa de consulta dos resultados.

---

## 📊 Consulta dos Resultados

Após a execução completa da DAG, é possível consultar os dados processados diretamente no banco PostgreSQL.

### Acessar o banco de dados

Execute o comando abaixo:

```bash
docker exec -it postgres_analytics psql -U airflow -d analytics
```

Este comando abrirá o terminal SQL do PostgreSQL dentro do container.

### Executar a consulta

No prompt do PostgreSQL, execute:

```sql
SELECT
    categoria,
    quantidade_produtos,
    preco_medio,
    preco_minimo,
    preco_maximo,
    atualizado_em
FROM metricas_categoria
ORDER BY categoria;
```

A consulta retornará as métricas agregadas por categoria geradas pelo pipeline de dados.
