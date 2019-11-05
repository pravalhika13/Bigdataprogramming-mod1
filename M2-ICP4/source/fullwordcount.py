import sys
import os

os.environ["SPARK_HOME"] = "C:\\Users\prava\Downloads\spark-2.4.4-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "C:\\Users\prava\Desktop\hadoop-winutils-2.6.0"
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
sc = SparkContext("local[2]", "NetworkWord")
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines_RDD = ssc.socketTextStream("localhost", 9999)

# Split each line into words
data_RDD = lines_RDD.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = data_RDD.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)


wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()