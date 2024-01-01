# TFG
En aquest repositori es guarden els scripts que s'han utilitzat en el treball de fi de grau: Sistema de monitoratge d’una xarxa mesh
sense fils amb Prometheus i Grafana.

Si teniu algún dubte, podeu contactar amb mi per: sandra.buitrago@estudiantat.upc.edu


Scripts del director del treball: \n
-- build-csv-tfg.py
-- jsonp-main.py
-- meshmon-main.py

Scripts antics o antigues proves del treball:
-- scriptprueba.py: JSON a Prometheus
-- scriptpush3.py: JSON a Prometheus
-- scriptpruebacsv.py: CSV a Prometheus
-- scriptfinalcsv.py: CSV a Prometheus
-- monitorjson2.sh: Tasca que executa el cron amb el subsistema inotify tools que monitoritza una carpeta
-- monitorjson.sh: Tasca que executa el cron en la que envia dades cada minut a Prometheus donant igual si s'enviaven noves dades o dades antigues

Scripts que s'estan utilitzant:
-- cambio.sh:  Tasca que executa el cron cada minut
-- scriptcambio.py: Script que transforma els CSV a format que accepta Prometheus.
