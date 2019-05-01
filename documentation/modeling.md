# Proyecto 3 (Big Data) - Minería de Texto con Spark

    Stiven Ramírez Arango - sramir70@eafit.edu.co
    Sebastián Ospina Cabarcas - sospin26@eafit.edu.co
    Camilo Suaza Gallego - csuazag@eafit.edu.co

# Modeling

<p align="center"> <img src="http://crisp-dm.eu/wp-content/uploads/2013/03/Modeling.jpg"> </p>

En esta cuarta fase se construyen los modelos del **índice invertido** y **clustering basado en métricas de similitud**.

## 1. Índice Invertido

El índice invertido es una estructura de datos que contiene la siguiente estructura:

| Palabra          | Lista de noticias (5 noticias de mayor frecuencia)   |
| ------------- |:-------------:|
| Palabra 1      | [(news1,f1), (news11,f11),(news4,f4),(news10,f10), … (news50,f50) ] |
| Palabra 2         | [(news2,f2), (news14,f14),(news1,f1),(news20,f20), … (news3,f3) ]      |
|  Palabra 3      | [(news50,f50), (news1,f1),(news11,f11),(news21,f21), … (news2,f2) ]      |
| ... | ... |
| Palabra N| |

En el índice invertido se tiene la frecuencia de cada palabra en el título + descripción, donde por cada palabra que se ingrese por teclado en el notebook de **Databricks**, se lista en orden descendente por frecuencia de palabra en el contenido <título> de la noticia, las noticias más relevantes. Se listan las 5 de mayor frecuencia <frec, news_id, tittle>.


- **frec**: Frecuencia de la palabra en la noticia < id > (incluye título y descripción).
- **id**: Id de la noticia.
- **tittle**: Título de la noticia.

### 1.1 Diseño

En esta etapa se deben tener los datos limpios, y esto ya se llevó a cabo en la fase anterior de **Data Preparation**.

Se selecciona los datos que se van a trabajar de la siguiente manera:

- id_noticia
- título_noticia_limpio
- contenido_noticia_limpio

Para ello realizamos una consulta SQL que muestre los datos a procesar:

```sql
SELECT * 
FROM dataCleaned
LIMIT 5; 
```

Se obtiene la siguiente tabla:

<p align="center"> <img src="https://user-images.githubusercontent.com/31974084/57049746-09859680-6c3f-11e9-8bb5-99ad62ba21c6.png"> </p>

### 1.2 Desarrollo

Se lee el dataframe con pyspark y se pone en el contexto de Python:

```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
dataFrame = sqlContext.sql("select id,tittle,content from dataCleaned")
```

Se convierte el dataframe a RDD (Resilient Distributed Dataset) para llevar a cabo actiones y transformaciones en dicho dataset:

```python
RDD = dataFrame.rdd
RDD = RDD.map(lambda line: (str(line[0]), str(line[1]), list(line[2])))
```
