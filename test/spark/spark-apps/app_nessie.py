from pyspark.sql import SparkSession

# Create a SparkSession (configuration will be picked up from spark-defaults.conf)
spark = SparkSession.builder.appName("Iceberg-Nessie-Write").getOrCreate()

# 1. Create Sample Data
data = [
    ("Alice", "Engineer", 5),
    ("Bob", "Data Scientist", 3),
    ("Charlie", "Manager", 8),
    ("David", "Analyst", 2),
]
df = spark.createDataFrame(data, ["name", "job_title", "experience_years"])

# 2. Write to Iceberg table (managed by Nessie)

# Option 1: Using DataFrame API
# df.writeTo("nessie.my_db.employees").createOrReplace()

# Option 2: Using Spark SQL
spark.sql("CREATE DATABASE IF NOT EXISTS nessie.my_db").show()

spark.sql("CREATE TABLE nessie.my_db.employees (name STRING, job_title STRING, experience_years INT) USING iceberg").show()

spark.sql("INSERT INTO nessie.my_db.employees VALUES ('Alice', 'Engineer', 5), ('Bob', 'Data Scientist', 3), ('Charlie', 'Manager', 8)")

# 3. Read from the Iceberg table (optional, to verify)
read_df = spark.read.table("nessie.my_db.employees")
read_df.show()

# Stop the SparkSession
spark.stop()