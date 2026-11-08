# Riwi QA Spark — Pipeline de calidad y análisis de transacciones

Pipeline por lotes con **Apache Spark (PySpark)** y **Python 3.11** que valida archivos de transacciones, separa los registros válidos de los rechazados (con el **motivo** de cada rechazo) y genera un resumen analítico por cliente. Incluye una suite de pruebas automatizadas con **Pytest**.

---

## 📋 Tabla de Contenidos

1. [Requisitos Previos](#-requisitos-previos)
2. [Estructura del Proyecto](#-estructura-del-proyecto)
3. [Instalación y Configuración](#-instalación-y-configuración)
4. [Ejecución del Pipeline](#-ejecución-del-pipeline)
5. [Ejemplo de Entrada y Salida](#-ejemplo-de-entrada-y-salida)
6. [Ejecución de Pruebas](#-ejecución-de-pruebas)
7. [Reglas de Validación](#-reglas-de-validación)
8. [Decisiones Técnicas](#-decisiones-técnicas)
9. [Informe Técnico](#-informe-técnico)
10. [Licencia](#-licencia)

---

## 🛠 Requisitos Previos

* **Python:** `3.11.x`
* **Java (JDK):** 8, 11 o 17 (obligatorio para la JVM de Spark 3.5). Con versiones más nuevas pueden aparecer errores al iniciar Spark.
* **pip** y soporte para entornos virtuales (`venv`)

```bash
python3 --version
java -version
```

---

## 📁 Estructura del Proyecto

```text
riwi-qa-spark/
├── datos/
│   └── transacciones.csv       # Dataset de ejemplo (1200 filas)
├── ejemplos/                   # CSV pequeño de entrada y su salida generada
├── src/
│   ├── __init__.py
│   └── validador.py            # Lectura, reglas, separación, resumen y CLI
├── tests/
│   ├── conftest.py             # Fixture de SparkSession compartida (scope=session)
│   └── test_pipeline.py        # Reglas, entradas inválidas y consistencia de extremo a extremo
├── INFORME.md                  # Investigación, fuentes y justificación de decisiones
├── LICENSE
├── pytest.ini
├── README.md
└── requirements.txt
```

---

## ⚙️ Instalación y Configuración

```bash
cd riwi-qa-spark
python3.11 -m venv .venv
source .venv/bin/activate        # En Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

`requirements.txt`:

```text
pyspark==3.5.6
pytest>=8,<9
```

Si Spark no encuentra Java, define `JAVA_HOME`:

```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
export PATH=$JAVA_HOME/bin:$PATH
```

---

## 🚀 Ejecución del Pipeline

Desde la raíz del proyecto:

```bash
python -m src.validador --input datos/transacciones.csv --output datos/resultados
```

| Argumento | Obligatorio | Descripción |
| :--- | :---: | :--- |
| `--input` | Sí | Ruta del CSV de entrada |
| `--output` | Sí | Directorio donde se guardan los resultados |
| `--max-valor` | No | Valor máximo permitido por transacción (por defecto `10000000`) |

El CSV debe tener las columnas `id`, `cliente_id` y `valor`. El orden no importa: se leen por nombre.

### Resultados generados

Spark escribe **directorios** con archivos `part-*`, no un archivo único. Dentro de `--output`:

| Carpeta | Formato | Contenido |
| :--- | :--- | :--- |
| `validos/` | Parquet | `id, cliente_id, valor` de las transacciones válidas |
| `rechazados/` | CSV | `id, cliente_id, valor_original, motivo` |
| `resumen_cliente/` | CSV | Por cliente: `num_transacciones, total, promedio, minimo, maximo` (solo válidas) |

Cada ejecución **sobrescribe** esas tres subcarpetas, y no toca nada más del directorio indicado.

### Códigos de salida

| Código | Significado |
| :---: | :--- |
| `0` | Proceso completado (**haya o no filas rechazadas**: separarlas es el trabajo del programa) |
| `1` | Problema con la entrada: archivo inexistente, vacío, sin filas o con columnas faltantes |
| `2` | Error inesperado |

---

## 🔎 Ejemplo de Entrada y Salida

Ejecución sobre `datos/transacciones.csv`:

```text
Leídos: 1200 | Válidos: 690 | Rechazados: 510
```

Rechazos por motivo:

| Motivo | Filas |
| :--- | ---: |
| cliente_id inválido (menor o igual a cero) | 107 |
| valor menor o igual a cero | 98 |
| valor con formato de moneda | 66 |
| id duplicado | 65 |
| id ausente o no numérico | 55 |
| cliente_id ausente o no numérico | 54 |
| valor ausente | 38 |
| valor no numérico | 27 |

En `ejemplos/` hay un CSV pequeño de entrada con la salida que genera, para comprobar el resultado a mano.

---

## 🧪 Ejecución de Pruebas

```bash
python -m pytest -v
```

Las 18 pruebas comparten una sola `SparkSession` (`tests/conftest.py`), así que el arranque de la JVM se paga una vez (la suite tarda alrededor de 30 segundos). Cubren:

* Cada regla de rechazo (incluido el caso válido), con CSV temporales reales.
* Columnas faltantes, columnas en otro orden, archivo inexistente, archivo vacío y archivo solo con encabezado.
* Consistencia de extremo a extremo: válidos + rechazados = leídos, y el resumen por cliente coincide con los válidos guardados.

---

## 🔍 Reglas de Validación

Cada fila recibe **un solo motivo**: el de la primera regla que incumple, en este orden. Sin motivo, la fila es válida.

| # | Motivo | Origen | Por qué importa |
| :-: | :--- | :--- | :--- |
| 1 | `id ausente o no numérico` | Nueva | Sin identificador no se puede auditar la transacción |
| 2 | `cliente_id ausente o no numérico` | Nueva | No se puede atribuir al resumen por cliente |
| 3 | `cliente_id inválido (menor o igual a cero)` | Nueva | El `0` es un valor centinela de "sin cliente" y crearía un grupo falso en el resumen |
| 4 | `valor ausente` | Original | Un monto vacío distorsiona sumas y promedios |
| 5 | `valor con formato de moneda` | Nueva | Ej. `$569,856`: dato recuperable, pero con problema de formato en el origen |
| 6 | `valor no numérico` | Original | Texto como `no_disponible` |
| 7 | `valor menor o igual a cero` | Original | Montos nulos o negativos no son ventas válidas |
| 8 | `valor excede el máximo permitido` | Nueva | Posible error de captura (ceros de más); umbral configurable |
| 9 | `id duplicado` | Nueva | Podría contar dos veces el mismo ingreso; se rechazan todas las copias |

---

## 🏗 Decisiones Técnicas

* **Lectura como texto + `try_cast` por nombre:** con `.schema(...)` Spark asigna los tipos por posición y no detecta un encabezado incorrecto. Leer como texto permite validar las columnas reales y distinguir un valor vacío de uno no numérico (se conserva `valor_original`).
* **Rechazar en vez de limpiar:** un valor como `$569,856` se rechaza y no se corrige, porque el validador no debe adivinar el formato (por ejemplo, el significado de la coma).
* **Un motivo por fila:** garantiza que cada fila cae en exactamente un lado y que válidos + rechazados = total.
* **`cache()` con `unpersist()`:** el DataFrame etiquetado alimenta varias acciones; el caché evita recalcular, y se libera en un `finally`.
* **Formatos de salida:** Parquet para válidos (tipado y columnar, pensado para análisis posteriores); CSV para rechazados y resumen (los revisa una persona).
* **Cierre seguro:** `spark.stop()` siempre se ejecuta en un `finally`, y el código de salida se devuelve después.
* **Rutas sin valores fijos:** entrada y salida se reciben por argumentos.

---

## 📘 Informe Técnico

La investigación (evaluación diferida, esquemas y nulos, costo de agrupar, uso de caché, formatos y cuándo Spark no es necesario), las fuentes y la justificación completa están en [INFORME.md](INFORME.md).

---

## 📄 Licencia

Este proyecto se distribuye bajo la licencia incluida en el archivo [LICENSE](LICENSE).

---

## 👨‍💻 Author

- GitHub: **[Danilo-Doria](https://github.com/Danilo-Doria)**
- LinkedIn: **[Danilo Doria Diaz](https://www.linkedin.com/in/danilo-doria-diaz/)**
- Mail: **danilodoria519@gmail.com**