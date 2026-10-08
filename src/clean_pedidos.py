import pyspark.sql.functions as F
from utils.spark_session import get_spark_session
from src.utils.logger import get_logger
import pandas as pd
from src.cotacao_dolar import extract
import json

logger = get_logger(__name__)

spark = get_spark_session()


def read_spark_csv(path_bronze_json)-> pd.DataFrame:
    logger.info(f"Carregando CSV: {path_bronze_json}")
    df = spark.read.option("multiLine", "true").json(path_bronze_json)
    logger.info("CSV carregado com sucesso com %d linhas", df.count())
    return df

def flags_erros_sql(df)-> pd.DataFrame:
    logger.info("Calculando erros da base de pedidos")
    df.createOrReplaceTempView("pedidos")

    pedidos = spark.sql(
        """ 
        SELECT
        *

        ,case
            when data_pedido is null then true
            when data_pedido = 'NULL'  then true
            when data_pedido = ''  then true
            when data_pedido like '%/%' then true
            else false
            end as flag_erro_data_pedido

        ,case
            when status is null then true
            when status = 'NULL'  then true
            when status = ''  then true
            else false
            end as flag_erro_status
        ,case
            when valor is null then true
            when valor = 'NULL'  then true
            when valor like '-%' then true
            else false
            end as flag_erro_valor
        FROM pedidos
        """
    )
    erros_data = pedidos.filter(F.col('flag_erro_data_pedido') == True).count()
    logger.info("Encontrato %d erros na coluna data_pedido", erros_data)
    
    erros_status = pedidos.filter(F.col('flag_erro_status') == True).count()
    logger.info("Encontrato %d erros na coluna status", erros_status)
    
    erros_valor = pedidos.filter(F.col('flag_erro_valor') == True).count()
    logger.info("Encontrato %d erros na coluna valor", erros_valor)
    
    total_erros = erros_data+erros_status+erros_valor
    total_linhas = df.count()
    pct = (total_erros/total_linhas)* 100
    pct = round(pct)
    return logger.info("Encontrato %d erros em um total de %d linhas que representam %.2f%% da base de pedidos", total_erros,total_linhas, pct)
    
def normalizacao_colunas_sql(df)-> pd.DataFrame:

    logger.info("Normalizando colunas com erros")
    df.createOrReplaceTempView("pedidos_silver")
    pedidos_silver = spark.sql("""
        SELECT
            cliente_id,

            CASE
                WHEN data_pedido IS NULL OR data_pedido = '' THEN NULL
                WHEN data_pedido LIKE '%/%'
                    THEN to_date(data_pedido, 'dd/MM/yyyy')
                ELSE to_date(data_pedido, 'yyyy-MM-dd')
            END AS data_pedido,

            pedido_id,

            CASE
                WHEN status IS NULL OR status = '' THEN 'sem dados'
                ELSE status
            END AS status,

            CASE
                WHEN valor IS NULL THEN 0
                ELSE ABS(CAST(valor AS DOUBLE))
            END AS valor

        FROM pedidos_silver
    """)
    return pedidos_silver

def adiciona_cotacao(df,url,path_json)-> pd.DataFrame:
    
    for i in range(3):
        status = extract(url,path='/opt/spark/data/bronze/cotacao.json')
        if status == 200:
            break
    logger.info("Extracao da cotacao do dolar concluida com sucesso")
    logger.info("Adicionando a cotacao ao dataframe")
    


    with open(
        path_json,
        encoding="utf-8",
    ) as arquivo:
        data = json.load(arquivo)

    cotacao = float(data['bid'])


    df = df.withColumn(
        "cotacao_dolar",
        F.lit(cotacao)
    )


    return df
    
        

def salvar_df(df,path)-> pd.DataFrame:
    logger.info("Salvando dataframe")
    df.write.mode("overwrite").parquet(path)
    return logger.info("Dataframe salvo com sucesso")

def clean_pedidos(path_bronze_json='/opt/spark/data/bronze/pedidos.json',path_json ='/opt/spark/data/bronze/cotacao.json',
                   url='https://economia.awesomeapi.com.br/json/last/USD-BRL',path_silver='/opt/spark/data/silver/pedidos/pedidos_silver.parquet'):
    logger.info("Iniciando limpeza de clientes")
    df = read_spark_csv(path_bronze_json)
    flags_erros_sql(df)
    clientes_silver = normalizacao_colunas_sql(df)
    clientes_silver = adiciona_cotacao(clientes_silver,url,path_json)
    salvar_df(clientes_silver,path_silver)
    logger.info("Arquivo final gerado em %s", path_silver)



    