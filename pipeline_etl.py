from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

# Inicializa a sessão paralela do Spark
spark = SparkSession.builder.appName("Cloud_ETL_Transform").getOrCreate()

# A leitura do Parquet é paralelizada: cada núcleo da CPU pega um bloco de dados
df_sales = spark.read.parquet("s3://bucket-da-empresa/raw_sales_data/")

# O Spark não faz isso sequencialmente. Ele aplica o particionamento em rede (shuffle)
# e calcula os agregados em paralelo nos nós do cluster.
df_transformed = df_sales \
   .filter(col("status") == "COMPLETED") \
   .groupBy("region_id") \
   .agg(sum("transaction_value").alias("total_revenue"))

# Escreve o resultado limpo de volta para o data lake ou data warehouse
df_transformed.write.mode("overwrite").parquet("s3://bucket-da-empresa/clean_sales_data/")
