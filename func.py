import io
import json
import logging

import pandas as pd
import requests
import oci
from datetime import datetime, timedelta
import time
from dateutil.relativedelta import relativedelta
import urllib.request
import urllib.parse
import json
import copy

from fdk import response
logger = logging.getLogger(__name__)

def get_configurations():
    rps = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient({}, signer=rps)
    return client

def get_previous_day():
    yesterday = datetime.now() - timedelta(days=1)
    
    # Converter para o formato 'YYYY-MM-DD'
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    today_str = datetime.now().strftime('%Y-%m-%d')
    return [yesterday_str, today_str]

def define_dates():
    today = datetime.today()
    day = today.day
    today = datetime.today() - timedelta(days=1)

    if day <= 20:
        # Até o dia 20, carregar do início do mês anterior até hoje
        first_day_last_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        start_date = first_day_last_month
    else:
        # Após o dia 20, carregar somente do início do mês atual
        start_date = today.replace(day=1)
    
    # O fim é sempre o dia atual
    end_date = today

    return [start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")]

def get_api_configs():
    url = "https://bi-api.logcomex.io/api/v1/details"
    valores = define_dates()

    # shipment-intel-cabotagem | shipment-intel-ocean-export-brazil
    headers = {}
    headers['exportacao'] = {
        "x-api-key": "350v4qN9QNPWwi0UzBeWIpr9eJYxXrkCNSRedIwA",
        "product-signature": "shipment-intel-ocean-export-brazil",
        "Content-Type": "application/json"
    }
    headers['cabotagem'] = {
        "x-api-key": "350v4qN9QNPWwi0UzBeWIpr9eJYxXrkCNSRedIwA",
        "product-signature": "shipment-intel-cabotagem",
        "Content-Type": "application/json"
    }
    
    payload = {}
    payload['exportacao'] = {
        "filters": [
            {
                "field": "data_operacao",
                "value": valores,
                "rule": "date_range",
                "rule_type": "prefix",
                "elastic_rule": ".keyword"
            }
        ],
        "page": 1,
        "size": 5000
    }
    payload['cabotagem'] = {
        "filters": [
            {
            "field": "dt_embarque",
            "value": valores
            }
        ],
        "page": 1,
        "size": 5000
    }

    return [url, headers, payload]

def _collect_full_range_data(url, headers, payload, last_page):
    full_jsons = []

    while payload['page'] < last_page:
        res = requests.post(url, headers=headers, json=payload)
        dados = res.json()
        full_jsons += dados['data']
        payload['page'] += 1

    return full_jsons
    
def get_data(url, real_headers, real_payload):
    all_data = {}
    headers = copy.copy(real_headers)
    payload = copy.copy(real_payload)
    for pay in payload.keys():
        logger.info('> Get Data:', pay)
        try:
            res = requests.post(url, headers=headers[pay], json=payload[pay]).json()
            dados = _collect_full_range_data(url, headers[pay], payload[pay], res['meta']['last_page'])
            df = pd.DataFrame(dados)
            all_data[pay] = df
        except Exception as e:
            logger.error('ERRO: ', str(e))
    return all_data

def save_to_bucket(all_data, client):
    for k in all_data.keys():
        try:
            pq = all_data[k].to_parquet()
            res = client.put_object(
                namespace_name=namespace,
                bucket_name=bucket,
                object_name=f"LOGCOMEX/DATA_{k.upper()}.parquet",
                put_object_body=pq
            )
            logger.info(f'Salvo LogComex_{k}.')
        except Exception as e:
            logger.error("ERRO: ", str(e))

def handler(ctx, data: io.BytesIO = None):
    logging.info("Começando function")

    try:
        logging.info("Pegando configs")
        client = get_configurations()
        url, headers, payload = get_api_configs()

        logging.info("Coletando dados")
        t0 = time.time()
        full_data = get_data(url, headers, payload)
        t1 = time.time()

        elapsed_seconds = t1 - t0
        hours = int(elapsed_seconds // 3600)
        minutes = int((elapsed_seconds % 3600) // 60)
        seconds = elapsed_seconds % 60
        tempo_total = f"Tempo levado: {hours:02}:{minutes:02}:{seconds:05.2f}"
        logging.debug(tempo_total)

        logging.info("Salvando no bucket")
        save_to_bucket(full_data, client)
    except Exception as e:
        logging.error(f"#> ERRO DA FUNCTION: {str(e)}")

    return response.Response(
        ctx, response_data=json.dumps(
            {"message": "Funcao terminada"}),
        headers={"Content-Type": "application/json"}
    )
