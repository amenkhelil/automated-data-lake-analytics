import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, round, to_date

# Initialisation et récupération des paramètres d'exécution
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = args['S3_BUCKET_NAME']
raw_path = f"s3://{bucket_name}/raw/"
processed_path = f"s3://{bucket_name}/processed/"

# Extraction des données brutes
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)
print(f"[INFO] Lignes brutes extraites : {df_raw.count()}")

# Transformation et règles de conformité :
# 1. Elimination des anomalies comptables (prix <= 0)
# 2. Imputation des valeurs de catégories manquantes
# 3. Imputation des quantités manquantes ou nulles
# 4. Cast strict des types et parsing des dates
# 5. Calcul de l'attribut dérivé Montant_Total
df_cleaned = df_raw.filter((col("Prix").isNotNull()) & (col("Prix") > 0)) \
    .withColumn("Categorie", when((col("Categorie").isNull()) | (col("Categorie") == ""), "Non spécifié").otherwise(col("Categorie"))) \
    .withColumn("Quantite", when((col("Quantite").isNull()) | (col("Quantite") == "NULL") | (col("Quantite") <= 0), 1).otherwise(col("Quantite").cast("int"))) \
    .withColumn("Prix", col("Prix").cast("double")) \
    .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd")) \
    .withColumn("Montant_Total", round(col("Prix") * col("Quantite"), 2))

print(f"[INFO] Lignes nettoyées prêtes pour le chargement : {df_cleaned.count()}")

# Chargement optimisé au format Apache Parquet
(df_cleaned.repartition(1)
    .write
    .mode("overwrite")
    .parquet(processed_path))

print("[INFO] Données Parquet persistées avec succès.")
job.commit()
