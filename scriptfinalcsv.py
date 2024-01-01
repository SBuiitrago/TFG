#!/usr/bin/env python
import csv
import gzip
import sys
import re
from prometheus_client import Gauge, CollectorRegistry, push_to_gateway

def normalize_metric_name(name):
    name = re.sub(r'[^a-zA-Z0-9_:]', '_', name)
    return name.lower().strip('_')

def reformat_data(data, registry, prefix=""):
    result = []
    for row in data:
        for key, value in row.items():
            metric_name = normalize_metric_name(key)
            label_name = 'value'
            metric = Gauge(metric_name, f'Descripción de {metric_name}', labelnames=[label_name], registry=registry)
            try:
                metric.labels(value=str(value)).set(value)
            except ValueError:
                metric.labels(value=str(value)).set(1)
            result.append(metric)
    return result

def read_csv(input_file):
    data = []
    with gzip.open(input_file, 'rt') as csvfile:
        csvreader = csv.DictReader(csvfile)
        current_data = []
        current_date = None

        for row in csvreader:
            # Verificar si hay un cambio en la columna "date"
            if current_date is None or row.get("date") != current_date:
                # Nuevo conjunto de datos encontrado, procesar el anterior
                if current_data:
                    data.append(current_data)
                current_data = []
                current_date = row.get("date")

            current_data.append(row)

        # Asegurarse de procesar el último conjunto de datos
        if current_data:
            data.append(current_data)
    return data

def main(input_file, job_name):
    registry = CollectorRegistry()
    all_metrics = []

    try:
        data = read_csv(input_file)
    except Exception as e:
        print(f"Error al leer el archivo CSV {input_file}: {e}")
        return

    for dataset in data:
        # Agregar métricas al registro
        all_metrics.extend(reformat_data(dataset, registry))

    try:
        # Push metrics to Prometheus Pushgateway
        push_to_gateway('localhost:9091', job=job_name, registry=registry)
        print("Métricas enviadas correctamente al Pushgateway.")
    except Exception as e:
        print(f"Error al enviar métricas al Pushgateway: {e}")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Uso: python script.py archivo.csv nombre_del_trabajo")
    else:
        input_file = sys.argv[1]
        job_name = sys.argv[2]
        main(input_file, job_name)

