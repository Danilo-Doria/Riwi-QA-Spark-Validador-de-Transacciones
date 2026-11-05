# Framework para ejecución de pruebas y gestión de fixtures.
import pytest

# Importación de los artefactos del código fuente bajo prueba.
from src.validador import SCHEMA, crear_spark, obtener_errores

@pytest.fixture(scope='module')
def spark():
    """
    Configura y administra el ciclo de vida de la sesión de Spark para los tests.
    
    - scope='module': La JVM e hilos de Spark se inicializan una única vez para
      todos los tests del archivo, optimizando drásticamente el tiempo de ejecución.
    - yield: Entrega el objeto 'sesion' a los tests (Setup) y suspende la ejecución.
    - sesion.stop(): Se ejecuta al finalizar todos los tests del módulo (Teardown).
    """
    sesion = crear_spark()
    yield sesion
    sesion.stop()

def test_datos_validos_no_generan_errores(spark):
    """
    Caso de prueba positivo: Valida que registros con valores positivos 
    y no nulos no sean marcados erróneamente como inválidos.
    """
    # Dataset sintético en memoria local de Python.
    datos = [(1, 101, 50000), (2, 102, 10000), (3, 103, 75000)]
    
    # TIPO DE OPERACIÓN: TRANSFORMACIÓN
    # Convierte la estructura de Python en un DataFrame distribuido bajo el esquema.
    df = spark.createDataFrame(datos, SCHEMA)
    
    # TIPO DE OPERACIÓN:
    # - obtener_errores(df): TRANSFORMACIÓN (agrega la regla de filtro al DAG).
    # - .count(): ACCIÓN (materializa la ejecución y retorna un entero al Driver).
    assert obtener_errores(df).count() == 0


def test_datos_invalidos_son_detectados(spark):
    """
    Caso de prueba negativo: Valida que se capturen valores negativos, 
    nulos (None) y ceros, verificando tanto el conteo como los identificadores.
    """
    # Dataset sintético con casos de borde (negativo, None, 0).
    datos = [
        (1, 101, 50000),   # Válido
        (2, 102, -20000),  # Inválido: Negativo
        (3, 103, None),    # Inválido: Nulo
        (4, 104, 0)        # Inválido: Cero
    ]
    
    # TIPO DE OPERACIÓN: TRANSFORMACIÓN
    # Generación del DataFrame distribuido de prueba.
    df = spark.createDataFrame(datos, SCHEMA)
    
    # TIPO DE OPERACIÓN: TRANSFORMACIÓN (Narrow)
    # Define la operación perezosa de filtrado.
    errores = obtener_errores(df)

    # TIPO DE OPERACIÓN: ACCIÓN
    # Dispara la ejecución del plan para contar las filas filtradas.
    assert errores.count() == 3
    
    # TIPO DE OPERACIONES COMBINADAS:
    # - .select('id'): TRANSFORMACIÓN (Narrow) - Proyecta únicamente la columna requerida.
    # - .collect(): ACCIÓN - Transfiere todas las filas resultantes desde los ejecutores 
    #   hacia el Driver en forma de lista de objetos pyspark.sql.Row.
    #   *NOTA DE DISEÑO*: Solo se utiliza aquí por ser un dataset diminuto en testing.
    ids = {fila['id'] for fila in errores.select('id').collect()}
    
    # Verificación de que los IDs detectados correspondan exactamente a los inválidos.
    assert ids == {2, 3, 4}