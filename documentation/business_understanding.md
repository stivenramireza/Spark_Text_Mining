# Proyecto 3 (Big Data) - Minería de Texto con Spark

    Stiven Ramírez Arango - sramir70@eafit.edu.co
    Sebastián Ospina Cabarcas - sospin26@eafit.edu.co
    Camilo Suaza Gallego - csuazag@eafit.edu.co

# Business Understanding

<p align="center"> <img src="http://crisp-dm.eu/wp-content/uploads/2013/03/Business-Understanding.jpg"> </p>

En esta primera fase se comprende el problema planteado de analítica de datos, aplicando los siguientes lineamientos:

- **Determinar los objetivos del negocio**: 
	- Solucionar un problema de analítica de datos mediante **Big Data Analytics**.

- **Evaluar la situación**:
	- Se realiza un inventario de recursos y se determina que **Apache Spark** y **Databricks** son las opciones más acertadas para conseguir los objetivos del negocio.
	- Los costos no representan un problema, pues **Databricks Community Edition** permite implementar en notebooks durante 14 días de forma gratuita.


- **Determinar los objetivos de la minería de datos**: 
 	- Buscar una palabra en un documento de noticias con base en el índice invertido y observar las 5 noticias más relacionadas con dicha palabra.
	- Buscar una noticia con base en un clustering de documentos y observar las 4 noticias más relacionadas con dicha noticia.

- **Producir el plan de proyecto**: 
	- Se establece un plan que consiste en dividir la implementación de los algoritmos entre los 3 integrantes del proyecto, en donde una persona se encarga de la limpieza de los datos, la otra crea un buscador con base en el índice invertido y la última clusteriza las noticias para encontrar sus similares.
