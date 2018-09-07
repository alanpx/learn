from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
df = spark.read.json("CPE_TRACK_2018090522.stat")
df.createOrReplaceTempView("cpe")
df1 = spark.sql("select seg.installedapp,count(uid) c from cpe where seg.installedapp!='' group by seg.installedapp order by c desc limit 10")
df1.show()
