from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="vendas_pipileni_dag",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pipileni","vendas"]
)

def vendas_pipileni_dag():
    @task()
    def clean_clientes():
        from src.clean_clientes import clean_clientes
        clean_clientes()
        
    @task()
    def clean_pedidos():
        from src.clean_pedidos import clean_pedidos
        clean_pedidos()
    clientes = clean_clientes()
    pedidos = clean_pedidos()

    [clientes,  pedidos]

vendas_pipileni_dag()