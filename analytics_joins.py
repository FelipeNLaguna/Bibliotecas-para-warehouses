import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, concat, lit

# 1. Resolve o aviso "Missing Python executable" forçando o uso do interpretador atual
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def run_distributed_parallel_joins():
    """
    O arcabouço SparkSession atua como intérprete que converte as direções lógicas declaradas 
    em um sofisticado Gráfico Acíclico Direcionado (DAG).
    """
    spark = SparkSession.builder \
       .appName("Cloud_Scale_Analytics_Joins") \
       .config("spark.sql.join.preferSortMergeJoin", "true") \
       .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
       .getOrCreate()

    # 2. CORREÇÃO CRÍTICA: Geração de dados massivos de forma distribuída (sem matar o Python)
    # spark.range() cria os dados diretamente nos executores da JVM.
    df_logs = spark.range(1, 10000000) \
        .withColumnRenamed("id", "user_id") \
        .withColumn("session_metrics", concat(lit("Client_"), col("user_id"), lit("_log"))) \
        .repartition(200)

    df_tiers = spark.range(1, 10000000) \
        .withColumnRenamed("id", "user_id") \
        .withColumn("subscription_tier", concat(lit("Tier_"), (col("user_id") % 3).cast("string")))

    # === ALGORITMO 1: PARALELISMO ASSIMÉTRICO VIA BROADCAST HASH JOIN ===
    print("Invocando o Broadcast Hash Join Assíncrono...")
    
    joined_via_broadcast = df_logs.join(broadcast(df_tiers), "user_id")
    joined_via_broadcast.show(5, truncate=False)

    # === ALGORITMO 2: DISTRIBUIÇÃO E PARALELISMO DENSO VIA SORT-MERGE JOIN ===
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    print("Invocando a Orquestração Distribuída do Sort-Merge Join...")
    
    joined_via_sort_merge = df_logs.join(df_tiers, "user_id")
    joined_via_sort_merge.show(5, truncate=False)
    
    # A explicitação de '.explain()' imprime o plano de execução físico
    print("Plano Físico do Sort-Merge Join:")
    joined_via_sort_merge.explain()

    spark.stop()

if __name__ == "__main__":
    run_distributed_parallel_joins()
