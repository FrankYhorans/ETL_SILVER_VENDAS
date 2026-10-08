import requests
import json
from src.utils.logger import get_logger

logger = get_logger(__name__)

def extract(url,path):
    logger.info("Iniciando extracao da cotacao do dolar")
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()

        data = data['USDBRL']
        arquivo = path

        with open(arquivo, 'w', encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)

        logger.info("Extracao da cotacao do dolar concluida com sucesso")
        return response.status_code

    else:
        logger.error("Erro na extracao da cotacao do dolar")
        return response.status_code
    