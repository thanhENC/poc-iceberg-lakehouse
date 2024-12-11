from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark

## DEFINE SENSITIVE VARIABLES
NESSIE_URI = 'http://nessie:19120/api/v1' 
WAREHOUSE = 's3a://finlake/' 
AWS_ACCESS_KEY = 'minio_access_key'
AWS_SECRET_KEY = 'minio_secret_key'
AWS_S3_ENDPOINT= 'http://minio:9000'


print(AWS_S3_ENDPOINT)
print(NESSIE_URI)
print(WAREHOUSE)

# conf = SparkConf() \
#      .set("spark.driver.cores", "1") \
#      .set("spark.driver.memory", "512m") \
#      .set("spark.executor.memory", "512m") \
#      .set("spark.executor.cores", "1")

# spark = SparkSession.builder \
#      .appName("NessieApp") \
#      .config(conf=conf) \
#      .getOrCreate()

conf = (
    pyspark.SparkConf()
        .setAppName('app_name')
        .set('spark.jars.packages','org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,software.amazon.awssdk:bundle:2.17.257,software.amazon.awssdk:url-connection-client:2.17.257')
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', NESSIE_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.s3.endpoint', AWS_S3_ENDPOINT)
        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
        .set("spark.sql.defaultCatalog", "nessie")
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)
)
# Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

#create database
spark.sql("create database if not exists nessie.DB1").show()
# Create a Table
spark.sql("CREATE TABLE if not exists nessie.DB1.test_name2 (name STRING) USING iceberg;").show()
print("Table created")

#Insert
spark.sql("INSERT INTO nessie.DB1.test_name2 VALUES ('name1'), ('name2'), ('name3')").toPandas()
print("Inserted data")

## Query the Data
spark.sql("SELECT * FROM nessie.DB1.test_name2;").show()