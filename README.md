BigDataProcessing
En este proyecto vamos a construir una arquitectura lambda para el procesamiento de datos recolectados desde antenas de telefonía movil. Una arquitectura lambda se separa en tres capas:

Speed Layer: Capa de procesamiento en streaming. Computa resultados en tiempo real y baja latencia.
Batch Layer: Capa de procesamiento por lotes. Computa resultados usando grande cantidades de datos, alta latencia.
Serving Layer: Capa encargada de servir los datos, es nutrida por las dos capas anteriores.
En la carpeta práctica se encuentra el proyecto para la ejecución de todo el proceso En el pdf Memoria práctica se explica el proceso de forma detallada asi como las gráficas obtenidas mediante apache superset
