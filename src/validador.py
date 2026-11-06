# Manejo de rutas agnóstico al sistema operativo (Windows, Linux, macOS).
from pathlib import Path

# Punto de entrada principal a la funcionalidad de Spark SQL y DataFrames.
from pyspark.sql import SparkSession

# Función para referenciar columnas de un DataFrame de forma programática.
from pyspark.sql.functions import col

import argparse

# Tipos de datos nativos del motor Catalyst de Spark para definir esquemas estáticos.
from pyspark.sql.types import (
    StructType,   # Representa la estructura de una fila completa (el esquema).
    StructField,  # Define los metadatos de una columna individual.
    IntegerType,  # Entero con signo de 32 bits (-2^31 a 2^31 - 1).
    LongType,     # Entero con signo de 64 bits (-2^63 a 2^63 - 1).
)

# Evita la inferencia automática (inferSchema=True), lo cual ahorra lecturas
# adicionales del archivo y previene errores de tipos en datos heterogéneos.
SCHEMA = StructType([
    # Formato: StructField(nombre_columna, tipo_dato, permite_nulos)
    StructField('id', IntegerType(), True),
    StructField('cliente_id', IntegerType(), True),
    StructField('valor', LongType(), True),
])

COLUMNAS_REQUERIDAS = {'id', 'cliente_id', 'valor'}

def validar_columnas(df, requeridas=COLUMNAS_REQUERIDAS):
    """
    Valida que el DataFrame contenga todas las columnas requeridas.

    Args:
        df: DataFrame de Spark.
        requeridas: Set de nombres de columnas obligatorias.

    Raises:
        ValueError: Si faltan columnas requeridas.
    """
    faltantes = requeridas - set(df.columns)
    if faltantes:
        raise ValueError(
            f"Faltan columnas requeridas: {faltantes}. "
            f"Columnas recibidas: {df.columns}"
        )
    return True

def crear_spark():
    """
    Inicializa o recupera la SparkSession activa.
    Configuración:
      - master('local[2]'): Ejecución local empleando 2 núcleos/hilos de CPU.
      - appName(...): Identificador de la aplicación en la Spark UI (puerto 4040).
    """
    return (
        SparkSession.builder
        .master('local[2]')
        .appName('Riwi-QA-Transacciones')
        .getOrCreate()
    )


def obtener_errores(df):
    """
    Registros cuyo valor es nulo, cero o negativo.
    
    TIPO DE OPERACIÓN: TRANSFORMACIÓN (Narrow / Estrecha)
    - No ejecuta cómputo de inmediato (Lazy Evaluation).
    - Agrega un nodo de filtro al grafo acíclico dirigido (DAG).
    - No genera 'shuffle' (no hay intercambio de datos entre nodos por red).
    """
    return df.filter(col('valor').isNull() | (col('valor') <= 0))

def parse_args():
    parser = argparse.ArgumentParser(
        description="Validador y Analítica de Transacciones con Spark"
    )
    # Construcción de la ruta relativa hacia el archivo CSV
    # Subir dos niveles desde este archivo e ingresar a /datos/transacciones.csv
    parser.add_argument(
        "--input",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "datos" / "transacciones.csv",
        help="Ruta del CSV de entrada"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "datos" / "salida",
        help="Directorio de salida"
    )
    return parser.parse_args()   

# PIPELINE
def main():
    args = parse_args()
    spark = crear_spark()
    
    try:    
        # TIPO DE OPERACIÓN: TRANSFORMACIÓN
        # spark.read crea un DataFrameReader y valida metadatos; no carga el archivo completo en RAM.
        df = (
            spark.read
            .option('header', True)  # La primera fila contiene los encabezados.
            .schema(SCHEMA)          # Aplica el esquema predefinido.
            .csv(str(args.input))          # Formato de entrada (requiere string).
        )

        # Patrón Fail-Fast: validar estructura antes de consumir cómputo
        validar_columnas(df)

        print('\n=== DATOS ORIGINALES ===')
        # TIPO DE OPERACIÓN: ACCIÓN
        # Dispara la ejecución del plan. Lee los datos, toma las primeras 20 filas 
        # y las envía al Driver para imprimirlas en consola.
        df.show()

        # TIPO DE OPERACIÓN: TRANSFORMACIÓN
        # Construye la expresión lógica de filtrado sobre 'df'.
        errores = obtener_errores(df)
        
        print('\n=== REGISTROS INVALIDOS ===')
        # TIPO DE OPERACIÓN: ACCIÓN
        # Dispara el filtrado y muestra los primeros 20 registros que fallaron la regla.
        errores.show()

        # TIPO DE OPERACIÓN: ACCIÓN
        # Computa la cantidad total de registros que coinciden con la condición.
        # Cada partición cuenta localmente y consolida la suma total en el Driver.
        total = errores.count()
        
        if total == 0:
            print('PASS: No hay transacciones invalidas')
        else:
            print(f'FAIL: Se encontraron {total} transacciones invalidas')

        args.output.mkdir(parents=True, exist_ok=True)
        df.write.csv(str(args.output), header=True, mode="overwrite")
            
    finally:
        # Detiene la sesión y libera los recursos de la JVM, RAM y puertos asociados.
        spark.stop()


if __name__ == '__main__':
    main()