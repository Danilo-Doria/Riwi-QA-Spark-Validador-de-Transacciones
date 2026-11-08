import pytest
from src.validador import leer_csv, agregar_motivo, ejecutar

ENCABEZADO = 'id,cliente_id,valor'


def escribir_csv(ruta, lineas, encabezado=ENCABEZADO):
    """Crea un CSV temporal con el encabezado y las líneas dadas."""
    ruta.write_text('\n'.join([encabezado, *lineas]) + '\n', encoding='utf-8')
    return ruta


def motivos(spark, tmp_path, lineas, **kw):
    """Pasa las líneas por leer_csv + agregar_motivo y devuelve los motivos (None = válido)."""
    csv = escribir_csv(tmp_path / 'in.csv', lineas)
    df = agregar_motivo(leer_csv(spark, csv), **kw)
    return [f['motivo'] for f in df.collect()]   # collect: aceptable con datos diminutos


# ---------- Reglas: una prueba por motivo ----------

@pytest.mark.parametrize('linea, esperado', [
    ('1,101,50000',        None),                                          # fila válida
    ('1,101,-20000',       'valor menor o igual a cero'),                  # regla original
    ('1,101,0',            'valor menor o igual a cero'),                  # límite: cero
    ('1,101,',             'valor ausente'),                               # celda vacía
    ('1,101,abc',          'valor no numérico'),                           # texto
    ('1,101,"$569,856"',   'valor con formato de moneda'),                 # regla nueva
    ('1,0,5000',           'cliente_id inválido (menor o igual a cero)'),  # regla nueva
    ('1,,5000',            'cliente_id ausente o no numérico'),
    (',101,5000',          'id ausente o no numérico'),
    ('1,101,99999999999',  'valor excede el máximo permitido'),            # regla nueva
])
def test_cada_regla(spark, tmp_path, linea, esperado):
    assert motivos(spark, tmp_path, [linea], valor_max=10_000_000) == [esperado]


def test_id_duplicado_rechaza_todas_las_copias(spark, tmp_path):
    # Dos filas con id 8 -> ambas rechazadas; la del id 9 sigue válida.
    resultado = motivos(spark, tmp_path, ['8,104,100', '8,105,200', '9,106,300'])
    assert sorted(m or 'OK' for m in resultado) == ['OK', 'id duplicado', 'id duplicado']


def test_valor_con_espacios_sobrantes_es_valido(spark, tmp_path):
    # Documenta la decisión: los espacios alrededor de un número se toleran.
    assert motivos(spark, tmp_path, ['1,101,622061 ']) == [None]


# ---------- Entrada incompleta o incorrecta ----------

def test_columnas_faltantes(spark, tmp_path):
    csv = escribir_csv(tmp_path / 'malo.csv', ['1,101,500'], encabezado='id,cliente,monto')
    with pytest.raises(ValueError, match='Faltan columnas'):
        leer_csv(spark, csv)


def test_columnas_en_otro_orden_se_leen_por_nombre(spark, tmp_path):
    # Prueba que NO se asigna por posición (el bug que motivó leer como texto).
    csv = escribir_csv(tmp_path / 'orden.csv', ['500,1,101'], encabezado='valor,id,cliente_id')
    fila = leer_csv(spark, csv).collect()[0]
    assert (fila['id'], fila['cliente_id'], fila['valor']) == (1, 101, 500)


def test_archivo_inexistente(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        leer_csv(spark, tmp_path / 'no_existe.csv')


def test_archivo_vacio(spark, tmp_path):
    vacio = tmp_path / 'vacio.csv'
    vacio.write_text('')
    with pytest.raises(ValueError):
        leer_csv(spark, vacio)


def test_solo_encabezado_sin_filas(spark, tmp_path):
    csv = escribir_csv(tmp_path / 'solo_enc.csv', [])
    with pytest.raises(ValueError, match='no contiene filas'):
        ejecutar(spark, csv, tmp_path / 'out')


# ---------- Consistencia de extremo a extremo ----------

LINEAS_E2E = [
    '1,101,1000', '2,101,2000', '3,102,500',          # 3 válidas
    '4,102,-5', '5,,300', '6,103,abc', '7,103,"$1,000"',
    '8,104,100', '8,105,200', '9,106,99999999999',    # 7 rechazadas
]


def test_resultados_corresponden_a_la_entrada(spark, tmp_path):
    csv = escribir_csv(tmp_path / 'in.csv', LINEAS_E2E)
    salida = tmp_path / 'out'

    conteos = ejecutar(spark, csv, salida, valor_max=10_000_000)

    # Los conteos coinciden y no se pierde ninguna fila.
    assert conteos == {'leidos': 10, 'validos': 3, 'rechazados': 7}
    assert conteos['validos'] + conteos['rechazados'] == conteos['leidos']

    # Lo que quedó en disco coincide con lo calculado.
    validos = spark.read.parquet(str(salida / 'validos'))
    rechazados = spark.read.option('header', True).csv(str(salida / 'rechazados'))
    resumen = spark.read.option('header', True).csv(str(salida / 'resumen_cliente'))

    assert validos.count() == 3
    assert rechazados.count() == 7
    assert 'motivo' in rechazados.columns

    # El resumen por cliente cuadra con los válidos.
    por_cliente = {int(r['cliente_id']): (int(r['num_transacciones']), int(r['total']))
                   for r in resumen.collect()}
    assert por_cliente == {101: (2, 3000), 102: (1, 500)}
    assert sum(t for _, t in por_cliente.values()) == sum(r['valor'] for r in validos.collect())


# ---------- Nuevo: el umbral es parametrizable ----------

def test_max_valor_configurable(spark, tmp_path):
    # El mismo valor es válido con el umbral por defecto y rechazado con uno más bajo.
    assert motivos(spark, tmp_path, ['1,101,5000']) == [None]
    assert motivos(spark, tmp_path, ['1,101,5000'], valor_max=1000) == ['valor excede el máximo permitido']