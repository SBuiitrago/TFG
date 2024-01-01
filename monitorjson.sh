#!/usr/bin/env bash

carpeta_a_monitorizar="$HOME/meshmon/mmdata"
script_python="$HOME/meshmon/scripts/meshmon-main.py"
csv_pre="$HOME/meshmon/scripts/jsonp-main.py"
csv_post="$HOME/meshmon/scripts/meshmon-main.py"
carpeta_destino="$HOME/tar"

cd "/home/sandra"


echo "running $0"

call_meshmon_main() {
    echo "----- $(date)"
    echo "Archivo modificado: $archivo_modificado"
    cmd="$script_python -d $carpeta_a_monitorizar -o $carpeta_destino -f graph_json -k $(ls $carpeta_a_monitorizar|tail -1)"
    echo "$cmd"
    $cmd
    if [ $? -eq 0 ]; then
	echo "El comando se ejecutó correctamente"	
    else
	echo "Hubo un error al ejecutar el comando"
    fi
}

call_csv() {
    cmd="./meshmon/scripts/jsonp-main.py -o csv-pre -f state -d tar -k $(ls tar|tail -1)"
    echo "calling $cmd"
    of=$($cmd | sed -n 's/writing cvs to \(.*\)$/\1/p')
    if [ -f "$of" ] ; then
	gzip $of
	cmd="./meshmon/scripts/build-csv-tfg.py -o csv-post -k $of.gz"
	echo "calling $cmd"
	$cmd
    fi
}
call_push(){
	output=$(call_csv|tail -1)
	echo "salida ? : $output"
	if [[ "$output" != "skip" ]]; then
	cmd="python3 scriptfinalcsv.py csv-post/$(ls csv-post|tail -1) guifisants"
	echo "ultimo de csv-post: $(ls csv-post|tail -1)"
	echo "calling $cmd"
	$cmd
	else
	   echo "Se ha hecho un skip y no se han pasado los datos al pushgateway de Prometheus"	
	fi
}


call_meshmon_main >> meshmon-main.log 2>&1
call_csv >> meshmon-main.log 2>&1
call_push >> meshmon-main.log 2>&1

