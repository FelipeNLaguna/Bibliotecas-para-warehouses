from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Inicializa o motor distribuído do Spark
spark = SparkSession.builder.appName("Parallel_Window_Functions").getOrCreate()

# === AJUSTE: Conjunto de dados preenchido com casos de teste ideais ===
data = [
    ("Mariana", "TI", 9500.00),
    ("Lucas", "TI", 8000.00),
    ("Pedro", "TI", 8000.00),  # Empate para testar o dense_rank
    ("Juliana", "TI", 6500.00),
    ("Carlos", "Vendas", 7000.00),
    ("Ana", "Vendas", 7000.00),  # Empate
    ("João", "Vendas", 4500.00),
    ("Fernanda", "RH", 5000.00),
    ("Paulo", "RH", 4200.00)
]

# Criação do DataFrame a partir da lista
df = spark.createDataFrame(data, ["nome", "departamento", "salario"])

# Define a janela particionada pelo departamento e ordenada pelo salário.
# O particionamento garante que a computação de 'Vendas', 'TI' e 'RH' 
# ocorram em paralelo, em executores distintos na nuvem.
window_spec = Window.partitionBy("departamento").orderBy(F.col("salario").desc())

# Calcula o ranking de salários dentro de cada departamento simultaneamente
df_ranked = df.withColumn("rank_departamento", F.dense_rank().over(window_spec))

df_ranked.show()

# Para encerrar a sessão graciosamente
spark.stop()
