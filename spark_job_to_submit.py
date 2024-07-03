#***1. Creating spark session object inturn it will create spark context object
from pyspark.sql import SparkSession
#spark=SparkSession.builder.master("local[*]").appName("Spark core learning WE41").getOrCreate()
spark=SparkSession.builder.appName("we41 application").getOrCreate()
#spark session object has the function related to core, sql (sql/hive)
sc=spark.sparkContext#just renaming the spark.sparkContext as sc for simplicity
sc.setLogLevel("INFO")#We can set the logger level to INFO/WARN/ERROR to see the detail information of every step or warning if any or only error if any
#2. Creating RDDs using multiple ways
#Create rdd from different sources (FS sources)
#BELOW CODE IS NOT PREFERRED WAY TO WRITE THE CODE AND ACHIVE RESULT
hadooplines= sc.textFile("hdfs://127.0.0.1:54310/user/hduser/empdata.txt")
rdd2=hadooplines.map(lambda x:x.split(","))
rdd3=rdd2.map(lambda x:int(x[4]))
print(rdd3.collect())
#[100000, 90000, 10000, 100000, 20000]
print(rdd3.reduce(lambda amt1,amt2:amt1 if amt1>amt2 else amt2))

#BELOW CODE IS PREFERRED WAY TO WRITE THE CODE AND ACHIVE RESULT
#df1= spark.read.csv("hdfs://127.0.0.1:54310/user/hduser/empdata.txt")
#df1.createOrReplaceTempView("view1")
#spark.sql("select max(cast(_c4 as int)) as max_sal from view1").show()
