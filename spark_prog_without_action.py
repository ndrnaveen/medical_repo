from pyspark.sql.session import SparkSession
spark=SparkSession.builder.master("local[*]").getOrCreate()
sc=spark.sparkContext#just renaming the spark.sparkContext as sc for simplicity
sc.setLogLevel("ERROR")#We can set the logger level to INFO/WARN/ERROR to see the detail information of every step or warning if any or only error if any
hadooplines= sc.textFile("hdfs://127.0.0.1:54310/user/hduser/empdata.txt")
maprdd=hadooplines.map(lambda x:x.split(","))
print(maprdd.collect()) #action--> trigger point for executing the RDD transformation
filterrdd=maprdd.filter(lambda x:int(x[4])>10000)#the filterrdd will not be materialized in memory at all
#only the DAG got created upto here
print(maprdd.collect())
#DAG will be executed only after I perform an action


