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

	temp_output=$(mktemp)
	$cmd > "$temp_output" 2>&1

	echo "salida de csv-post: $temp_output"
    fi
}
call_push(){
	echo "salida ? : $temp_output"
	
	if [ -z "$temp_output" ] ; then
            echo "Se ha hecho un skip, las métricas no se envian"
            return 1
        fi


	cmd="python3 scriptcambio.py csv-post/$(ls csv-post|tail -1) guifisants"
	echo "ultimo de csv-post: $(ls csv-post|tail -1)"
	echo "calling $cmd"
	$cmd

	cmd="python3 scriptcambiocont.py csv-post/$(ls csv-post|tail -1) contador"
	$cmd
 
	rm "$temp_output"
}


call_meshmon_main > meshmon-main2.log 2>&1
call_csv >> meshmon-main2.log 2>&1
call_push >> meshmon-main2.log 2>&1

