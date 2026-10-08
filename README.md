# Pipeline de dados de vendas

Projeto de estudo para ler e tratar dados de clientes e pedidos com PySpark, consultar a cotação USD/BRL e coordenar tarefas com Apache Airflow usando Docker Compose.

## Tecnologias

- **Python e PySpark**: leitura e transformação dos dados.
- **Apache Spark**: motor de processamento de dados.
- **Apache Airflow**: interface e agendamento/coordenação das tarefas.
- **PostgreSQL**: banco interno usado pelo Airflow para guardar metadados, como DAGs e execuções.
- **Docker Compose**: inicialização dos serviços e configuração da rede entre containers.

## Pré-requisitos

- Docker Desktop instalado e em execução, com suporte ao comando `docker compose`.
- A imagem `eng-spark:3.5` disponível localmente. O Compose usa essa imagem para iniciar o Spark Master e o Worker; ela não é construída por este projeto.
- Acesso à internet para consultar a API de cotação USD/BRL.

## Estrutura do projeto

```text
.
├── dags/
│   └── vendas_pipileni_dag.py
├── data/
│   ├── bronze/       # Dados de entrada e arquivo da cotação
│   └── silver/       # Resultados tratados em Parquet
├── logs/
│   └── airflow/      # Logs de execução das DAGs
├── src/
│   ├── clean_clientes.py
│   ├── clean_pedidos.py
│   ├── cotacao_dolar.py
│   └── utils/
├── .env
├── Dockerfile.airflow
└── docker-compose.yml
```

## Configuração

O arquivo `.env` define quais pastas locais serão montadas nos containers:

```dotenv
APP_JOBS_PATH=./jobs
APP_RESOURCES_PATH=./src
APP_STORAGE_PATH=./data
APP_LOG_PATH=./logs
APP_METRICS_PATH=./metrics
```

Os dados de entrada esperados pelos scripts são:

- `data/bronze/clientes.csv`
- `data/bronze/pedidos.json`

O arquivo `data/bronze/cotacao.json` é criado/atualizado pelo script que consulta a API. Os resultados tratados são gravados em diretórios Parquet dentro de `data/silver/`.

Não publique o arquivo `.env` se ele contiver credenciais ou outros valores privados. As credenciais definidas diretamente no Compose são para uso local e devem ser substituídas antes de qualquer implantação compartilhada ou de produção.

## Iniciar os serviços

Execute na pasta onde está o `docker-compose.yml`:

```bash
docker compose up -d --build
```

O comando constrói a imagem customizada do Airflow e inicia os serviços. Na primeira execução, o Docker precisa baixar as imagens base e isso pode levar alguns minutos.

Abra a interface do Airflow em <http://localhost:8082>. As credenciais iniciais são `airflow` / `airflow`, a menos que `AIRFLOW_ADMIN_USER` e `AIRFLOW_ADMIN_PASSWORD` tenham sido definidos no ambiente.

Para listar os serviços e seu estado:

```bash
docker compose ps
```

Para acompanhar os logs do Scheduler:

```bash
docker compose logs -f airflow-scheduler
```

Para parar e remover os containers sem apagar o volume de metadados do Airflow:

```bash
docker compose down
```

O volume nomeado `airflow-postgres-data` mantém os metadados entre reinicializações. Evite `docker compose down -v` se quiser preservá-los.

## Acessos locais

| Serviço | Endereço no computador | Uso |
|---|---|---|
| Airflow | <http://localhost:8082> | Interface para visualizar e iniciar DAGs |
| Spark Master | <http://localhost:8080> | Interface do coordenador do Spark |
| Spark Worker | <http://localhost:8081> | Interface do processo que executa tarefas Spark |

Os containers também se comunicam pela rede interna `spark-net`. Nessa rede, o endereço do Master é `spark://spark-master:7077`; os containers usam o nome do serviço em vez de `localhost`.

## DAG de vendas

O arquivo `dags/vendas_pipileni_dag.py` define a DAG `vendas_pipileni_dag` com duas tarefas:

- `clean_clientes`: importa e executa `clean_clientes` de `src/clean_clientes.py`.
- `clean_pedidos`: importa e executa `clean_pedidos` de `src/clean_pedidos.py`.

A DAG está configurada sem agendamento automático (`schedule_interval=None`), então deve ser iniciada manualmente pela interface do Airflow. O Scheduler identifica os arquivos Python da pasta `dags/`.

## Observações sobre a integração com Spark

O Compose inicia um Spark Master e um Spark Worker, e a imagem do Airflow inclui Java, PySpark e o provider Apache Spark.

**Na configuração atual, porém, os scripts importam `get_spark_session()` de `src/utils/spark_session.py`, cuja sessão usa `.master("local[*]")`.** Isso faz o processamento executar localmente dentro do container que roda a tarefa Airflow; não submete o processamento ao Spark Master. Para executar distribuído no Worker, será necessário ajustar essa sessão ou submeter os jobs com `spark-submit` para `spark://spark-master:7077`.

A conexão Airflow `spark_default` está configurada para esse endereço, mas sua existência, por si só, não muda o modo de execução usado pelos scripts Python.

Além disso, a DAG atual não declara dependência entre as tarefas: ambas são criadas, mas o código não define uma ordem explícita entre elas. Se uma tarefa precisar aguardar a outra, isso deverá ser expresso na DAG.

## Permissões dos dados

O serviço `airflow-storage-permissions` do Compose prepara as pastas `data/bronze` e `data/silver` para escrita pelo usuário do Airflow. Isso é necessário porque os scripts gravam a cotação e substituem resultados Parquet nessas pastas.

Se uma tarefa falhar com `PermissionError` ou com erro ao limpar um diretório Parquet, confira os logs e confirme que esse serviço terminou com sucesso:

```bash
docker compose ps -a airflow-storage-permissions
docker compose logs airflow-storage-permissions
```
