# Riwi QA Spark — Validador de Transacciones

Pipeline de control de calidad de datos (*Data Quality Assurance*) implementado con **Apache Spark (PySpark)** y **Python 3.11**. El sistema valida registros financieros transaccionales aplicando reglas de negocio estrictas, esquemas predefinidos (*schema enforcement*) y una suite de pruebas unitarias automatizadas con **Pytest**.

---

## 📋 Tabla de Contenidos

1. [Requisitos Previos](#-requisitos-previos)
2. [Estructura del Proyecto](#-estructura-del-proyecto)
3. [Instalación y Configuración](#-instalación-y-configuración)
4. [Ejecución del Pipeline](#-ejecución-del-pipeline)
5. [Ejecución de Pruebas Unitarias](#-ejecución-de-pruebas-unitarias)
6. [Arquitectura y Decisiones Técnicas](#-arquitectura-y-decisiones-técnicas)
7. [Reglas de Validación](#-reglas-de-validación)
8. [Licencia](#-licencia)

---

## 🛠 Requisitos Previos

Antes de configurar el entorno virtual, asegúrate de contar con las siguientes dependencias en tu sistema operativo:

* **Python:** `3.11.18` (compatible con `3.11.x`)
* **Java (JDK):** `OpenJDK 17` (obligatorio para el runtime de Apache Spark / JVM)
* **Gestor de paquetes:** `pip` y soporte para entornos virtuales (`venv`)

Verifica tus versiones instaladas ejecutando en la terminal:

```bash
python3 --version
java -version
```

---

## 📁 Estructura del Proyecto

```text
riwi-qa-spark/
├── datos/
│   └── transacciones.csv       # Dataset fuente de transacciones a validar
├── src/
│   ├── __init__.py
│   └── validador.py            # Pipeline principal, esquema estático y lógica de filtrado
├── tests/
│   └── test_validador.py       # Pruebas unitarias y fixture de SparkSession con pytest
├── LICENSE                     # Licencia del proyecto
├── README.md                   # Documentación técnica del repositorio
└── requirements.txt            # Dependencias fijadas del entorno
```

---

## ⚙️ Instalación y Configuración

1. **Ubicarse en la raíz del proyecto:**

   ```bash
   cd "/home/danilo/Documents/Riwi 2026/complements/Spark/riwi-qa-spark"
   ```

2. **Crear y activar el entorno virtual (`.venv`):**

   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate
   ```

3. **Verificar el archivo `requirements.txt`:**

   ```text
   pyspark==3.5.6
   pytest>=8,<9
   ```

4. **Instalar las dependencias:**

   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

5. **Configurar `JAVA_HOME` (si no está definido en tu `.bashrc` o `.zshrc`):**

   ```bash
   export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
   export PATH=$JAVA_HOME/bin:$PATH
   ```

---

## 🚀 Ejecución del Pipeline

El script principal lee el archivo `datos/transacciones.csv`, aplica el esquema explícito `SCHEMA` y reporta en consola tanto las transacciones originales como aquellas que incumplen las reglas de negocio.

Ejecuta el módulo desde la raíz del proyecto:

```bash
python -m src.validador
```

### Salida esperada en consola

```text
=== DATOS ORIGINALES ===
+---+----------+-----+
| id|cliente_id|valor|
+---+----------+-----+
|  1|       101|50000|
|  2|       102|-2000|
|  3|       103| NULL|
|  4|       104|    0|
+---+----------+-----+

=== REGISTROS INVALIDOS ===
+---+----------+-----+
| id|cliente_id|valor|
+---+----------+-----+
|  2|       102|-2000|
|  3|       103| NULL|
|  4|       104|    0|
+---+----------+-----+

FAIL: Se encontraron 3 transacciones invalidas
```

---

## 🧪 Ejecución de Pruebas Unitarias

La suite de pruebas automatizadas en `tests/test_validador.py` utiliza **Pytest** (`8.4.2`) e instancia una única sesión de Spark compartida a nivel de módulo (`@pytest.fixture(scope='module')`) para reducir la sobrecarga de arranque de la JVM.

Para ejecutar todas las pruebas desde la raíz del proyecto:

```bash
python -m pytest -v
```

---

## 🏗 Arquitectura y Decisiones Técnicas

* **Schema Enforcement (`StructType`):** Se define el esquema de datos explícitamente en lugar de utilizar `inferSchema=True`. Esto evita que Spark realice una lectura adicional completa sobre el archivo CSV para adivinar los tipos de datos, mejorando el rendimiento y garantizando consistencia de tipos (`IntegerType` y `LongType`).
* **Evaluación Perezosa (*Lazy Evaluation*):** La función `obtener_errores(df)` construye una transformación estrecha (*narrow transformation*) mediante `.filter()`, la cual se evalúa en memoria por partición sin generar intercambio de datos por red (*shuffle*).
* **Rutas Agnósticas (`pathlib.Path`):** El acceso a `datos/transacciones.csv` se resuelve dinámicamente usando `Path(__file__).resolve().parents[1]`, asegurando que el script funcione sin importar el directorio de trabajo desde el que se invoque.
* **Cierre Seguro (`try / finally`):** El bloque `finally: spark.stop()` garantiza la liberación inmediata de hilos, memoria RAM y puertos locales incluso ante fallos inesperados durante la ejecución.

---

## 🔍 Reglas de Validación

El validador inspecciona cada fila bajo la siguiente estructura y criterios de calidad:

| Columna | Tipo de Dato (Spark) | Nullable | Regla de Rechazo | Descripción del Error |
| :--- | :--- | :---: | :--- | :--- |
| `id` | `IntegerType()` | `True` | — | Identificador único de la transacción |
| `cliente_id` | `IntegerType()` | `True` | — | Identificador del cliente asociado |
| `valor` | `LongType()` | `True` | `col('valor').isNull() \| (col('valor') <= 0)` | Monto nulo, igual a cero o negativo |

---

## 📄 Licencia

Este proyecto se distribuye bajo la licencia incluida en el archivo [LICENSE](LICENSE).

---

## 👨‍💻 Author

- GitHub: **[Danilo-Doria](https://github.com/Danilo-Doria)**
- LinkedIn: **[Danilo Doria Diaz](https://www.linkedin.com/in/danilodd)**
- Mail: **danilodoria519@gmail.com**