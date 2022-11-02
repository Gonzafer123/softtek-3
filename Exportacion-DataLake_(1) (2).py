# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import count
from pyspark.sql.functions import avg, sum

# COMMAND ----------

storage = "input0111"
containerstorage = "entrada"
keyStorage = "FP5gt2TXNjZXKFT8x2cCQT4Li+BSu+ns1kErJf54kdUNcoVvzyKSwSJrzZRJTdi8kno3FNaX7IQ2+AStMq18Yg=="

datalake = "datalake0111"
containerDatalake = "salida"
keyDatalake = "HOy05zfeCp24FRrFtcdoyFuOy4f9ih5jp+OK+58nVbv01tdKmiAM/SIZYjMNsi69XftyizlKLpEH+AStoaA+Wg=="


# COMMAND ----------

dbutils.fs.mount(
source = f"wasbs://{containerstorage}@{storage}.blob.core.windows.net",
mount_point = "/mnt/tpFinal",
extra_configs = {f"fs.azure.account.key.{storage}.blob.core.windows.net":keyStorage})

# COMMAND ----------

# Importaión Tabla Categoría
dfCategoria = spark.read.options(header='True', inferSchema='True', delimiter=',') \
.csv("/mnt/tpFinal/dboCategoria.csv")

# COMMAND ----------

dfCategoria.display()

# COMMAND ----------

#Transformación 1
dfCategoriaColumnRenamed = dfCategoria.withColumnRenamed("Categoria", "Nombre_Categoria")
dfCategoriaColumnRenamed.show()

# COMMAND ----------

list_df_export_csv={}
list_df_export_csv['dfCategoriaColumnRenamed']=dfCategoriaColumnRenamed

# COMMAND ----------


# Importaión Tabla Fact Mine
dfFactMine = spark.read.options(header='True', inferSchema='True', delimiter=',') \
.csv("/mnt/tpFinal/dboFactMine.csv")
dfFactMine.display()

# COMMAND ----------

# Transformación 2
dfFactMineSumOreMined=dfFactMine.select(sum(dfFactMine.TotalOreMined).alias("AllTotalOreMined"))
dfFactMineSumOreMined.show()


# COMMAND ----------

# list_dt_export_csv.append(dfFactMineSumOreMined)

list_df_export_csv['dfFactMineSumOreMined']=dfFactMineSumOreMined

# COMMAND ----------

# Importaión Tabla Mine
dfMine = spark.read.options(header='True', inferSchema='True', delimiter=',') \
.csv("/mnt/tpFinal/dboMine.csv")
dfMine.display()
dfMine.printSchema()

# COMMAND ----------

# Transofrmación 3
dfMineSelected =dfMine.select("Country", "FirstName","LastName","Age")
dfMine.display()


# COMMAND ----------

# list_dt_export_csv.append(dfMineSelected)

list_df_export_csv['dfMineSelected']=dfMineSelected

# COMMAND ----------

# Transformación 4
dfMineTotalWastedByCountry = dfMine.groupBy("Country").agg(sum("TotalWasted").alias("Total By Country"))
dfMineTotalWastedByCountry.show()

# COMMAND ----------

# list_dt_export_csv.append(dfMineTotalWastedByCountry)

list_df_export_csv['dfMineTotalWastedByCountry']=dfMineTotalWastedByCountry

# COMMAND ----------

# Importaión Tabla Producto
dfProducto = spark.read.options(header='True', inferSchema='True', delimiter=',') \
.csv("/mnt/tpFinal/dboProducto.csv")
dfProducto.display()

# COMMAND ----------

# Transformación 5
dfTotalProductos = dfProducto.select(sum(dfProducto.Cod_Producto).alias("Total productos disponibles"))
dfTotalProductos.show()

# COMMAND ----------

# list_dt_export_csv.append(dfTotalProductos)

list_df_export_csv['dfTotalProductos']=dfTotalProductos


# COMMAND ----------

# Transformación 6
dfCodProductoDesc = dfProducto.sort(col("Cod_Producto").desc())
dfCodProductoDesc.show()

# COMMAND ----------

# list_dt_export_csv.append(dfCodProductoDesc)

list_df_export_csv['dfCodProductoDesc']=dfCodProductoDesc


# COMMAND ----------

# Importaión Tabla SubCategoria
dfSubCategoria = spark.read.options(header='True', inferSchema='True', delimiter=',') \
.csv("/mnt/tpFinal/dboSubCategoria.csv")
dfSubCategoria.display()

# COMMAND ----------

# Transformación 7

dfCodCategoriaFiltered = dfSubCategoria.filter(col("Cod_Categoria") == 3)
dfCodCategoriaFiltered.display()
# Probar mostrar a qué categoría pertenece mediante un Inner Join

# COMMAND ----------

# list_dt_export_csv.append(dfCodCategoriaFiltered)

list_df_export_csv['dfCodCategoriaFiltered']=dfCodCategoriaFiltered


# COMMAND ----------

# Importaión Tabla VentasInternet
dfVentasInternet = spark.read.options(header='True', inferSchema='True', delimiter=',') \
.csv("/mnt/tpFinal/dboVentasInternet.csv")
dfVentasInternet.display()

# COMMAND ----------

# Transformación 8
dfIngresosNetosInternet = dfVentasInternet.withColumn('Ingresos Netos', (col('Cantidad') * col('PrecioUnitario')) - col('CostoUnitario'))
#  Revisar de redondear
dfIngresosNetosInternet.display()

# COMMAND ----------

# list_dt_export_csv.append(dfIngresosNetosInternet)

list_df_export_csv['dfIngresosNetosInternet']=dfIngresosNetosInternet


# COMMAND ----------

#Transformación 9
dfIngresosNetosPromTotal = dfIngresosNetosInternet.groupBy("Cod_Producto").agg(sum("Ingresos Netos").alias("Suma Total Ingresos Netos") ,avg("Ingresos Netos").alias("Promedio Total Ingresos Netos"))
dfIngresosNetosPromTotal.show()

# COMMAND ----------

# list_dt_export_csv.append(dfIngresosNetosPromTotal)

list_df_export_csv['dfIngresosNetosPromTotal']=dfIngresosNetosPromTotal

# COMMAND ----------

dbutils.fs.mount(
source = f"wasbs://{containerDatalake}@{datalake}.blob.core.windows.net",
mount_point = "/mnt/tpFinalOut2",
extra_configs = {f"fs.azure.account.key.{datalake}.blob.core.windows.net":keyDatalake})

# COMMAND ----------

#Este funciona

def export_csv(df, filePath, fileName):
    temp_path = f"{filePath}/__temp"
    target_path = f"{filePath}/{fileName}.csv"

    df.coalesce(1).write.mode("overwrite").option("header",True).csv(temp_path)

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path

    # get the part file generated by spark write
    fs = Path(temp_path).getFileSystem(sc._jsc.hadoopConfiguration())
    csv_part_file = fs.globStatus(Path(temp_path + "/part*"))[0].getPath()

    # move and rename the file
    fs.rename(csv_part_file, Path(target_path))
    fs.delete(Path(temp_path), True)


# COMMAND ----------

# toExportcsv in list_df_export_csv:
    # export_csv(toExportcsv, filePath, toExportcsv.name)


for toExportcsvName, toExportcsv in list_df_export_csv.items():
    export_csv(toExportcsv, "/mnt/tpFinalOut2", toExportcsvName)

