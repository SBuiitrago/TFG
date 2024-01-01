#!/bin/bash
export PATH=/usr/bin:/bin:/home/sandra/meshmon/scripts/parser/meshmon-main.py

export carpeta_a_monitorizar="/home/sandra/meshmon/mmdata"

export script_python="/home/sandra/meshmon/scripts/parser/meshmon-main.py"
export carpeta_destino="/home/sandra/tar"

# Ruta al archivo de bloqueo
archivo_bloqueo="/tmp/monitorprueba.lock"

# Función para manejar la limpieza al salir
limpiar_y_salir() {
    rm -f "$archivo_bloqueo"
    exit
}

# Verificar si el archivo de bloqueo ya existe
if [ -e "$archivo_bloqueo" ]; then
    echo "Otra instancia del script ya está en ejecución. Saliendo."
    exit 1
fi

# Configurar la función de limpieza al atrapar señales de salida
trap limpiar_y_salir EXIT

# Crear el archivo de bloqueo
touch "$archivo_bloqueo"

archivo_modificado=$(/usr/bin/inotifywait -e modify,create --format "%w%f" -q -r "$carpeta_a_monitorizar" 2>&1)
    if [ -n "$archivo_modificado" ]; then
        echo "Archivo modificado: $archivo_modificado"
	/usr/bin/python3 "$script_python"-d "$carpeta_a_monitorizar"  -o "$carpeta_destino" -f graph-json "$archivo_modificado"
    	if [ $? -eq 0 ]; then
   		 echo "El comando se ejecutó correctamente"
	else
    		echo "Hubo un error al ejecutar el comando"
	fi
    fi

