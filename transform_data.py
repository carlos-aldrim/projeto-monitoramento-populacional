from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
from pyspark.sql.functions import when
import os

# Configuração de ambiente para compatibilidade
if os.name == 'nt':
    os.environ["HADOOP_HOME"] = "C:\\winutils"
else:
    os.environ["SPARK_LOCAL_DIRS"] = "/app/artifacts"

# Criar diretórios necessários
os.makedirs("/app/artifacts", exist_ok=True)
os.makedirs("/app/datalake/spark-temp", exist_ok=True)
os.makedirs("/app/datalake/processed/cleaned", exist_ok=True)

# Iniciar SparkSession
spark = SparkSession.builder \
    .appName("ETL_Transformacao") \
    .config("spark.hadoop.io.nativeio.checked", "false") \
    .getOrCreate()

# Ler parquet bruto do datawarehouse
print("🔍 Lendo dados do datawarehouse...")
df = spark.read.parquet("./datawarehouse")
df.printSchema()

print("📊 Dados antes da limpeza:")
df.show(5)

# Remover coluna desnecessária '__index_level_0__', se existir
if "__index_level_0__" in df.columns:
    print("🚫 Removendo coluna '__index_level_0__' desnecessária...")
    df = df.drop("__index_level_0__")

# Aplicar transformações:
# - remover duplicatas
# - preencher valores nulos simples
# - padronizar colunas em minúsculas
print("🧹 Iniciando limpeza e padronização dos dados...")
df_clean = df.dropDuplicates() \
    .na.fill({
        "id": "",
        "sigla": "",
        "nome": "",
        "regiao": ""
    }) \
    .withColumn("sigla", lower(col("sigla"))) \
    .withColumn("nome", lower(col("nome"))) \
    .withColumn("regiao", lower(col("regiao")))

# Padronizar valores da coluna 'regiao'
df_clean = df_clean.withColumn(
    "regiao",
    when(col("regiao").isin("1", "n", "norte"), "norte")
    .when(col("regiao").isin("2", "ne", "nordeste"), "nordeste")
    .when(col("regiao").isin("3", "co", "centro-oeste"), "centro-oeste")
    .when(col("regiao").isin("4", "s", "sul"), "sul")
    .when(col("regiao").isin("5", "se", "sudeste"), "sudeste")
    .otherwise(col("regiao"))
)


print("✅ Dados após limpeza e transformação:")
df_clean.printSchema()
df_clean.show(5)

# Gravar parquet final da etapa 3
output_path = "./datalake/processed/cleaned"
print(f"💾 Salvando dados tratados em: {output_path}")
df_clean.write.mode("overwrite").parquet(output_path)

# Criar flag de sucesso
with open(os.path.join(output_path, "_PROCESSAMENTO_FINALIZADO.txt"), "w") as f:
    f.write("Processamento concluído com sucesso!\n")

# Encerrar sessão Spark
spark.stop()
print("✅ Etapa 3 finalizada com sucesso!")
