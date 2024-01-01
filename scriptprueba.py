#!/usr/bin/env python
import json
import sys
import re
import gzip
from prometheus_client import Gauge, CollectorRegistry, push_to_gateway
from datetime import datetime

def normalize_metric_name(name):
    name = re.sub(r'[^a-zA-Z0-9:_]', '_', name)
    if name[0].isdigit():
        name = 'metric_' + name
    return name

def reformat_data(data, registry, prefix="", result=None, file_date=None):
    if result is None:
        result = []

    if isinstance(data, dict):
        for key, value in data.items():
            result.extend(reformat_data(value, registry, prefix + key + "_", file_date=file_date))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            result.extend(reformat_data(item, registry, prefix + f'{i}_', file_date=file_date))
    else:
        try:
            float_value = float(data)
        except ValueError:
            float_value = data

        metric_name = normalize_metric_name(f'{prefix[:-1]}')

        metric_name_with_date = f'{metric_name}_fecha'
        metric = Gauge(metric_name_with_date, f'Descripción de {metric_name} con fecha', registry=registry, labelnames=['value', 'fecha'])
        metric.labels(value=str(data), fecha=file_date).set(1)
        result.append(metric)

    return result

def main(input_file, job_name):
    registry = CollectorRegistry()
    try:
        # Corrige la expresión regular para extraer la fecha del nombre del archivo
        match = re.search(r'(\d{2}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})', input_file)
        file_date = match.group(1) if match else datetime.now().strftime("%y-%m-%d_%H-%M-%S")

        with gzip.open(input_file, 'rt') as file:
            data = json.load(file)
    except FileNotFoundError:
        print(f"El archivo {input_file} no se encontró.")
        return
    except json.JSONDecodeError:
        print(f"No se pudo analizar el archivo JSON {input_file}. Asegúrate de que sea un archivo JSON válido.")
        return

    all_metrics = reformat_data(data, registry, file_date=file_date)

    # Push metrics to Prometheus Pushgateway with job_name including date and time
    push_to_gateway('localhost:9091', job=f'{job_name}_{file_date}', registry=registry)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Uso: python script.py archivo.json nombre_del_trabajo")
    else:
        input_file = sys.argv[1]
        job_name = sys.argv[2]
        main(input_file, job_name)

