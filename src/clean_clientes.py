import pyspark.sql.functions as F
from utils.spark_session import get_spark_session
from src.utils.logger import get_logger
import pandas as pd

logger = get_logger(__name__)

spark = get_spark_session()


def read_spark_csv(path)-> pd.DataFrame:
    logger.info(f"Carregando CSV: {path}")
    df = (spark.read
               .option("Header", "True")
               .option("inferSchema","True")
               .option("sep",",")
               .csv(path)
               )
    logger.info("CSV carregado com sucesso com %d linhas", df.count())
    return df

def flags_erros_sql(df)-> pd.DataFrame:
    logger.info("Calculando erros da base clientes")
    df.createOrReplaceTempView("clientes")
    clientes = spark.sql("""
                select
                *
                ,CASE when nome is null then true else false
                        End as flag_nome_invalido
                ,CASE when email is null then true 
                        when right(email,4) <> '.com' then true 
                        else false
                        End as flag_email_invalido
                ,CASE when data_cadastro is null then true 
                        when data_cadastro like '%/%' then true
                        else false
                        End as flag_data_cadastro_invalido
                ,CASE when telefone = 'NULL' then true 
                        when telefone is null then true
                        WHEN TRIM(telefone) RLIKE '^[0-9]{10,11}$' THEN false
                        else true
                        End as flag_telefone_invalido
                ,CASE when cidade is null then true else false
                        End as flag_cidade_invalido
                from clientes
                """)
    
    qtd_nome_invalido = clientes.filter(F.col("flag_nome_invalido") == True).count()
    logger.info("Encontrato %d erros na coluna nome", qtd_nome_invalido)
    qtd_flag_email_invalido = clientes.filter(F.col("flag_email_invalido") == True).count()
    logger.info("Encontrato %d erros na coluna email", qtd_flag_email_invalido)
    qtd_flag_data_cadastro_invalido = clientes.filter(F.col("flag_data_cadastro_invalido") == True).count()
    logger.info("Encontrato %d erros na coluna data_cadastro", qtd_flag_data_cadastro_invalido)
    qtd_flag_telefone_invalido = clientes.filter(F.col("flag_telefone_invalido") == True).count()
    logger.info("Encontrato %d erros na coluna telefone", qtd_flag_telefone_invalido)
    qtd_flag_cidade_invalido = clientes.filter(F.col("flag_cidade_invalido") == True).count()
    logger.info("Encontrato %d erros na coluna cidade", qtd_flag_cidade_invalido)

    total_erros = qtd_nome_invalido+ qtd_flag_email_invalido+ qtd_flag_data_cadastro_invalido+ qtd_flag_telefone_invalido+ qtd_flag_cidade_invalido
    
    total_linhas = clientes.count()
    
    pct = (total_erros/ total_linhas) * 100
    pct = round(pct) 
    return logger.info("Encontrato %d erros em um total de %d linhas que representam %.2f%% da base de clientes", total_erros,total_linhas, pct)
    
def normalizacao_colunas_sql(df)-> pd.DataFrame:
    df.createOrReplaceTempView("clientes_silver")
    logger.info("Normalizando colunas com erros")
    clientes_silver = spark.sql("""
        SELECT
            id,
            CASE
                WHEN nome is null then split(email, '@')[0] 
                else nome
            END AS nome,
            CASE
                WHEN email is null then concat(nome,"@email.com")
                else email
            END AS email,
            CASE
                WHEN data_cadastro IS NULL OR data_cadastro = '' THEN NULL
                WHEN data_cadastro LIKE '%/%'
                    THEN to_date(data_cadastro, 'dd/MM/yyyy')
                ELSE to_date(data_cadastro, 'yyyy-MM-dd')
            END AS data_cadastro,
            CASE
                when telefone is null then null
                when TRIM(telefone) RLIKE '^[0-9]{10,11}$' then telefone
                else null
            END AS telefone

        FROM clientes_silver
    """)
    return clientes_silver

def salvar_df(df,path)-> pd.DataFrame:
    logger.info("Salvando dataframe")
    df.write.mode("overwrite").parquet(path)
    return logger.info("Dataframe salvo com sucesso")

def clean_clientes(path='/opt/spark/data/bronze/clientes.csv',path_silver = '/opt/spark/data/silver/clientes/clientes_silver.parquet'):
    logger.info("Iniciando limpeza de clientes")
    df = read_spark_csv(path)
    flags_erros_sql(df)
    clientes_silver = normalizacao_colunas_sql(df)
    salvar_df(clientes_silver,path_silver)
    logger.info("Arquivo final gerado em %s", path_silver)



    