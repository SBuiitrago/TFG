#!/usr/bin/env python
import csv
import gzip
import sys
import re
from prometheus_client import Counter, CollectorRegistry, push_to_gateway

PREVIOUS_DATA = "prevdata.csv.gz"

def normalize_metric_name(name):
    name = re.sub(r'[^a-zA-Z0-9_:]', '_', name)
    return name.lower().strip('_')

#Guarda los datos para utilizar el valor previo
def guardar_datos(data):
    # Guarda los datos en un archivo CSV comprimido en formato gzip
    with gzip.open(PREVIOUS_DATA, 'wt', newline='') as csvfile:
        fieldnames = data[0].keys() if data else []  # Utiliza las claves del primer diccionario como encabezados
        csvwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if fieldnames:
            csvwriter.writeheader()

        for row in data:
            csvwriter.writerow(row)


def reformat_data(data, prevdata, registry, prefix=""):
    result = []

    for prev_row, current_row in zip(prevdata, data):

        for (prev_key, prev_value), (key, value) in zip(prev_row.items(), current_row.items()):
            metric_name = normalize_metric_name(key)
            label_name = 'value'
            metric = Counter(metric_name, f'Descripción de {metric_name}', labelnames=[label_name], registry=registry)

            try:
                if prev_key == key:
                    if prev_value is not None:
                        difference = float(value) - float(prev_value)
                        metric.labels(value=str(value)).inc(difference)
                    else: 
                        metric.labels(value=str(value)).inc(float(value))
            except ValueError:
                metric.labels(value=str(value)).inc(1)

            result.append(metric)

    return result




def read_csv(input_file):
    data = []
    with gzip.open(input_file, 'rt') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            data.append(row)
    return data

def main(input_file, job_name):
    registry = CollectorRegistry()
    all_metrics = []

    try:
        data = read_csv(input_file)
    except Exception as e:
        print(f"Error al leer el archivo CSV {input_file}: {e}")
        return

    try:
        prevdata = read_csv(PREVIOUS_DATA)
    except Exception as e:
        print(f"Error al leer el archivo previo {input_file}: {e}")
        return

    all_metrics.extend(reformat_data(data, prevdata, registry))
    guardar_datos(data)

    try:
        # Push metrics to Prometheus Pushgateway
        push_to_gateway('localhost:9091', job=job_name, registry=registry)
        print("Métricas enviadas correctamente al Pushgateway.")
    except Exception as e:
        print(f"Error al enviar métricas al Pushgateway: {e}")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Uso: python script.py archivo.csv.gz nombre_del_trabajo")
    else:
        input_file = sys.argv[1]
        job_name = sys.argv[2]
        main(input_file, job_name)

