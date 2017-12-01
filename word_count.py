from pyspark.sql import SparkSession
  
# Start a Spark session
spark = SparkSession\
  .builder\
  .appName("word-count")\
  .getOrCreate()
  
# Look at 'books' directory on HDFS
!hadoop fs -ls books

# Read book text into an RDD called 'lines'
lines = spark.read.text("books")\
  .rdd.map(lambda r: r[0])

# Look at the first line
lines.first()

# Find word counts (runs on cluster)
from operator import add
counts = lines.flatMap(lambda x: x.split(' ')) \
  .map(lambda x: (x, 1)) \
  .reduceByKey(add)

# Print out the counts (runs on driver)
output = counts.collect()
for (word, count) in output:
  print("%s: %i" % (word, count))

