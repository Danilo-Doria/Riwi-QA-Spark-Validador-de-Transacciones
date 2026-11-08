import pytest
from src.validador import crear_spark


@pytest.fixture(scope='session')
def spark():
    """
    Una sola SparkSession para toda la suite. Arrancar la JVM cuesta varios
    segundos, así que se paga una vez y todos los tests la comparten.
    """
    sesion = crear_spark()
    yield sesion       # los tests usan la sesión aquí
    sesion.stop()      # se cierra al terminar toda la suite