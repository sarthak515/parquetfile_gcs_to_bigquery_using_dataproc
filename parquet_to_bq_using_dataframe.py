


# Import pySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

bucket = "sarthak11"
spark.conf.set('temporaryGcsBucket', bucket)

# Create DataFrame
#columns= ["employee_name", "department", "salary"]
df =spark.read.parquet('gs://sarthak11/data_parq.parquet')
#df = spark.createDataFrame(data = data, schema = columns)
df2 = df.distinct()
df2.write.format("bigquery") \
  .option("table", "mlconsole-poc.sarthak.srh") \
  .mode("append") \
  .save()

