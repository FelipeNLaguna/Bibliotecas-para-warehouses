from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Inicializa o motor distribuído do Spark
spark = SparkSession.builder.appName("Parallel_Window_Functions").getOrCreate()

data =
df = spark.createDataFrame(data, ["nome", "departamento", "salario"])

# Define a janela particionada pelo departamento e ordenada pelo salário.
# O particionamento garante que a computação de 'Vendas' e 'TI' 
# ocorram em paralelo, em executores distintos na nuvem.
window_spec = Window.partitionBy("departamento").orderBy(F.col("salario").desc())

# Calcula o ranking de salários dentro de cada departamento simultaneamente
df_ranked = df.withColumn("rank_departamento", F.dense_rank().over(window_spec))

df_ranked.show()
