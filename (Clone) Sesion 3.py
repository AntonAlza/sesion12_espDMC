# Databricks notebook source

!pip install pyspark

# !pip install NOMBRE_LIBRERIA_VERSION



# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

# MAGIC %md
# MAGIC AÑADIR TEXTO

# COMMAND ----------

# MAGIC %md
# MAGIC Ingresar Texto

# COMMAND ----------

import pyspark
import pandas as pd

# COMMAND ----------


spark.read.table("ventas_tienda_antony")


# COMMAND ----------

# DBTITLE 1,CARGAR LA TABLA SOBRE UN DF

df_ventas = spark.read.table("ventas_tienda_antony")


# COMMAND ----------

type(df_ventas)

# COMMAND ----------

df_ventas.printSchema()

# COMMAND ----------

# DBTITLE 1,Opción 1
df_ventas.show()

# COMMAND ----------

# DBTITLE 1,Opción 2

df_ventas.display()

# COMMAND ----------

df_ventas.select("Tienda", "Marca").display()

# COMMAND ----------

#SELECCIONAR COLUMNAS ESPECIFICAS

df_ventas.select("Tienda","Genero").show()

# COMMAND ----------


#Agregar Columna calculada

df_ventas.withColumn("Venta_USD",df_ventas["precio_venta"]/3.75)

# COMMAND ----------

df_ventas.show()

# COMMAND ----------

df_ventas = df_ventas.withColumn("Venta_USD",df_ventas["precio_venta"]/3.75)

# COMMAND ----------

df_ventas.show()

# COMMAND ----------

# MAGIC %md
# MAGIC EJERCICIO:
# MAGIC
# MAGIC Caragar el archivo "Ejercicio_Venta"
# MAGIC
# MAGIC Crear Columna "precio total" -> cantidad * precio
# MAGIC
# MAGIC Crear Columna impuesto, que va a ser el 18% del precio total
