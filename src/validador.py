"""
Pipeline de calidad y análisis de transacciones con PySpark.

Flujo: leer CSV -> validar columnas -> etiquetar cada fila con un motivo de
rechazo (o null si es válida) -> separar válidos/rechazados -> resumen por
cliente -> guardar resultados.
"""
import argparse
import sys
from pathlib import Path   # Rutas independientes del sistema operativo

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, expr, lit, when

# Columnas que el CSV debe traer sí o sí (el orden no importa).
COLUMNAS_REQUERIDAS = {'id', 'cliente_id', 'valor'}

# Umbral por defecto; se puede cambiar con --max-valor.
MAX_VALOR = 10_000_000


def validar_columnas(df, requeridas=COLUMNAS_REQUERIDAS):
    """
    Comprueba que el DataFrame tenga todas las columnas requeridas.
    Lanza ValueError con las que faltan y las que llegaron.
    Solo mira los metadatos del DataFrame: no lee datos ni dispara cómputo.
    """
    faltantes = requeridas - set(df.columns)
    if faltantes:
        raise ValueError(
            f"Faltan columnas requeridas: {faltantes}. "
            f"Columnas recibidas: {df.columns}"
        )
    return True


def leer_csv(spark, ruta):
    """
    Lee el CSV como texto, valida el encabezado real y castea por nombre.

    Por qué no se usa .schema(...): Spark asignaría los tipos por posición e
    ignoraría los nombres del encabezado, así que un CSV con columnas mal
    nombradas o en otro orden pasaría sin error.

    Devuelve: id, cliente_id, valor (tipados) y valor_original (texto crudo,
    para distinguir "vacío" de "no numérico" más adelante).
    """
    if not Path(ruta).exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {ruta}")

    # TRANSFORMACIÓN (lazy): sin schema ni inferSchema, todo llega como string.
    crudo = spark.read.option('header', True).csv(str(ruta))

    # Fail-fast: se valida la estructura antes de gastar cómputo.
    validar_columnas(crudo)

    # try_cast devuelve null si el texto no se puede convertir, sin lanzar error
    # (igual en Spark 3.x y en 4.x, donde ANSI está activo por defecto).
    return crudo.select(
        expr('try_cast(id as int)').alias('id'),
        expr('try_cast(cliente_id as int)').alias('cliente_id'),
        expr('try_cast(valor as bigint)').alias('valor'),
        col('valor').alias('valor_original'),
    )


def crear_spark():
    """
    Crea (o recupera) la SparkSession.
      - local[2]: ejecución local con 2 hilos; no hace falta un clúster.
      - shuffle.partitions=4: el valor por defecto (200) es excesivo para datos
        pequeños; el shuffle de groupBy y ventanas crearía 200 tareas casi vacías.
    """
    return (
        SparkSession.builder
        .master('local[2]')
        .appName('Riwi-QA-Transacciones')
        .config('spark.sql.shuffle.partitions', '4')
        .getOrCreate()
    )


def agregar_motivo(df, valor_max=MAX_VALOR):
    """
    Agrega la columna 'motivo': el texto de la PRIMERA regla incumplida, o null
    si la fila es válida. Así cada fila queda en un solo lado (válida o
    rechazada) y válidos + rechazados = total.

    El orden de los when() importa: gana el primero que se cumple. Las
    comprobaciones de null van antes de las comparaciones numéricas, porque
    comparar con null no da verdadero ni falso.

    TRANSFORMACIÓN wide: la ventana por 'id' obliga a un shuffle para juntar
    las filas con el mismo id.
    """
    # Cuántas filas comparten el mismo id (para detectar duplicados).
    veces_id = count(lit(1)).over(Window.partitionBy('id'))

    motivo = (
        when(col('id').isNull(), 'id ausente o no numérico')
        .when(col('cliente_id').isNull(), 'cliente_id ausente o no numérico')
        # 0 o negativo es un valor centinela de "sin cliente".
        .when(col('cliente_id') <= 0, 'cliente_id inválido (menor o igual a cero)')
        # valor null Y texto original null -> la celda venía vacía.
        .when(col('valor').isNull() & col('valor_original').isNull(), 'valor ausente')
        # Texto que empieza con símbolo de moneda, ej. "$569,856".
        .when(col('valor').isNull() & col('valor_original').rlike(r'^\s*[$€£]'),
              'valor con formato de moneda')
        # Cualquier otro texto que no se pudo convertir, ej. "no_disponible".
        .when(col('valor').isNull(), 'valor no numérico')
        .when(col('valor') <= 0, 'valor menor o igual a cero')
        .when(col('valor') > valor_max, 'valor excede el máximo permitido')
        # Última regla: se rechazan todas las copias, porque no hay criterio
        # para saber cuál es la correcta.
        .when(veces_id > 1, 'id duplicado')
        # Sin .otherwise(): las filas válidas quedan con motivo = null.
    )
    return df.withColumn('motivo', motivo)


def separar(etiquetado):
    """
    Divide el DataFrame etiquetado en válidos (motivo null) y rechazados.
    Los rechazados conservan valor_original (el texto crudo) como evidencia,
    porque 'valor' sería null en casos como "$569,856".
    TRANSFORMACIÓN narrow (filter y select): sin shuffle.
    """
    validos = (etiquetado.filter(col('motivo').isNull())
               .select('id', 'cliente_id', 'valor'))
    rechazados = (etiquetado.filter(col('motivo').isNotNull())
                  .select('id', 'cliente_id', 'valor_original', 'motivo'))
    return validos, rechazados


def resumen_por_cliente(validos):
    """
    Resumen de las transacciones válidas por cliente: cantidad, total,
    promedio, mínimo y máximo, ordenado por total descendente.
    TRANSFORMACIÓN wide: groupBy mueve todas las filas de un cliente a la misma
    partición (shuffle), que es la operación costosa de este pipeline.
    """
    return (validos.groupBy('cliente_id')
            .agg(F.count('*').alias('num_transacciones'),
                 F.sum('valor').alias('total'),
                 F.round(F.avg('valor'), 2).alias('promedio'),
                 F.min('valor').alias('minimo'),
                 F.max('valor').alias('maximo'))
            .orderBy(F.col('total').desc()))


def ejecutar(spark, entrada, salida, valor_max=MAX_VALOR):
    """
    Pipeline completo. Devuelve los conteos (dict) para poder probarlo.
    Lanza FileNotFoundError / ValueError si la entrada es inválida.
    """
    salida = Path(salida)
    df = leer_csv(spark, entrada)

    # cache(): 'etiquetado' alimenta varias acciones (3 count y 3 escrituras).
    # Sin caché, cada una repetiría lectura, casteo y la ventana con shuffle.
    # Costo: memoria, por eso se libera con unpersist() en el finally.
    etiquetado = agregar_motivo(df, valor_max).cache()
    try:
        n_total = etiquetado.count()   # ACCIÓN: ejecuta el plan y llena el caché
        if n_total == 0:
            raise ValueError("El archivo no contiene filas de datos")

        validos, rechazados = separar(etiquetado)
        resumen = resumen_por_cliente(validos)

        n_validos = validos.count()          # ACCIÓN
        n_rechazados = rechazados.count()    # ACCIÓN

        # Validación de negocio (no es un assert, que se puede desactivar con -O).
        if n_validos + n_rechazados != n_total:
            raise RuntimeError("Inconsistencia: se perdieron filas al separar")

        # Rutas propias dentro de --output: overwrite solo toca estas tres.
        salida.mkdir(parents=True, exist_ok=True)

        # ACCIONES (write): materializan y guardan.
        # Parquet para válidos: columnar, tipado, ideal para análisis posteriores.
        validos.write.mode('overwrite').parquet(str(salida / 'validos'))
        # CSV para rechazados y resumen: los revisa una persona.
        rechazados.write.mode('overwrite').option('header', True).csv(str(salida / 'rechazados'))
        resumen.write.mode('overwrite').option('header', True).csv(str(salida / 'resumen_cliente'))

        print(f'Leídos: {n_total} | Válidos: {n_validos} | Rechazados: {n_rechazados}')
        # ACCIONES (show): muestran el detalle en consola.
        rechazados.groupBy('motivo').count().orderBy(F.col('count').desc()).show(truncate=False)
        resumen.show(10)

        return {'leidos': n_total, 'validos': n_validos, 'rechazados': n_rechazados}
    finally:
        etiquetado.unpersist()   # libera la memoria del caché aunque haya error


def parse_args():
    """Define los argumentos de línea de comandos. No hay rutas fijas."""
    parser = argparse.ArgumentParser(
        description="Validador y analítica de transacciones con Spark"
    )
    parser.add_argument("--input", type=Path, required=True,
                        help="Ruta del CSV de entrada")
    parser.add_argument("--output", type=Path, required=True,
                        help="Directorio donde se guardan los resultados")
    parser.add_argument("--max-valor", type=int, default=MAX_VALOR,
                        help="Valor máximo permitido por transacción")
    return parser.parse_args()


def main():
    """
    Punto de entrada. Códigos de salida:
      0 = éxito (haya o no filas rechazadas: separarlas es el trabajo del programa)
      1 = problema con la entrada (archivo inexistente, vacío o columnas faltantes)
      2 = error inesperado
    """
    args = parse_args()
    if args.max_valor <= 0:
        print("ERROR: --max-valor debe ser mayor que cero", file=sys.stderr)
        sys.exit(1)

    spark = crear_spark()
    codigo = 0
    try:
        ejecutar(spark, args.input, args.output, args.max_valor)
        print('Proceso finalizado correctamente')
    except (FileNotFoundError, ValueError) as e:
        print(f'ERROR de entrada: {e}', file=sys.stderr)
        codigo = 1
    except Exception as e:
        print(f'ERROR inesperado: {type(e).__name__}: {e}', file=sys.stderr)
        codigo = 2
    finally:
        spark.stop()   # siempre libera JVM, memoria y puertos
    sys.exit(codigo)   # va después del finally para que stop() siempre corra


if __name__ == '__main__':
    main()