import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as func


spark = SparkSession.builder.appName("Cognitivo").getOrCreate()

# // 1. Conversão do formato dos arquivos para formato colunar
csvFile="file:///home/parallels/Documents/cognitivo/data/input/users/load.csv"
userSchema = "id INT, name STRING, email INT, phone INT, address STRING, age INT, create_date timestamp, update_date timestamp"
df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").schema(userSchema).load(csvFile)
df.show
df.write.format("parquet").mode("overwrite").save("file:///home/parallels/Documents/cognitivo/data/output/usersParquet")

# 2. Deduplicação dos dados convertidos
overId = Window.partitionBy(df.id).orderBy(df.update_date.desc())
ranked = df.withColumn("rank", func.rank().over(overId)).filter("rank == 1")

# 3. Conversão do tipo dos dados deduplicados
ranked.select("age", "create_date", "update_date").write.format("parquet").mode("overwrite").save("file:///home/parallels/Documents/cognitivo/data/output/resultParquet")

# Apenas para verificar que o dado é "carregavel"
resultSchema =  "age INT, create_date timestamp, update_date timestamp"
result = spark.read.schema(resultSchema).parquet("file:///home/parallels/Documents/cognitivo/data/output/resultParquet")
result.show()
