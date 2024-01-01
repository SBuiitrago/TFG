#!/usr/bin/env python
import json
import sys
import re
import gzip
from prometheus_client import Gauge, CollectorRegistry, push_to_gateway

def normalize_metric_name(name):
    # Es canviien els caracters no valids amb guions baixos, si el primer caracter no es valid s'afegeix metric abans del nom
    name = re.sub(r'[^a-zA-Z0-9:_]', '_', name)
    if name[0].isdigit():
        name = 'metric_' + name
    return name

#Canvieem el format de les dades del fitxer JSON a un format que Prometheus accepta
def reformat_data(data, registry, prefix="", result=None):
    if result is None:
        result = []

    if isinstance(data, dict):
        for key, value in data.items():
            result.extend(reformat_data(value, registry, prefix + key + "_"))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            result.extend(reformat_data(item, registry, prefix + f'{i}_'))
    #Comprovem que les dades float siguin float.
    else:
        try:
            float_value = float(data)
        except ValueError:
            float_value = data
        #Comprovem que el nom es valid y si no ho es o normalitzem
        metric_name = normalize_metric_name(f'{prefix[:-1]}')
        #Enviem les dades a Prometheus com a metrica Gauge (pot incrementar o decrementar)
        metric = Gauge(metric_name, f'Descripción de {metric_name}', registry=registry, labelnames=['value'], )
        metric.labels(value=str(data)).set(1)
        result.append(metric)

    return result

def save_metrics_to_file(metrics, output_file):
    with open(output_file, 'w') as file:
        for metric in metrics:
            file.write(f'{metric}\n')

def main(input_file, job_name):
    registry = CollectorRegistry()
    try:
        with gzip.open(input_file, 'rt') as file:
            
            data = json.load(file)
    except FileNotFoundError:
        print(f"El archivo {input_file} no se encontró.")
        return
    except json.JSONDecodeError:
        print(f"No se pudo analizar el archivo JSON {input_file}. Asegúrate de que sea un archivo JSON válido.")
        return

    all_metrics = reformat_data(data, registry)

    # Push metrics to Prometheus Pushgateway
    push_to_gateway('localhost:9091', job=job_name, registry=registry)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Uso: python script.py archivo.json nombre_del_trabajo")
    else:
        input_file = sys.argv[1]
        job_name = sys.argv[2]
        main(input_file, job_name)

