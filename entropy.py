from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import sum
from pyspark.sql.functions import max
import __builtin__
  
# Start a Spark session
spark = SparkSession\
  .builder\
  .appName("misp-del")\
  .getOrCreate()
  
# Get all the files for a given passage and high MOI
def find_files(passage_number, replicate_number, moi='high'):
  # Read metadata into an RDD so we can query it
  metadata = spark.read.format("csv").option("header", "true").load("misp/CHIKV_CARIB_metadata.csv")
  metadata.count()

  # Find files for given criteria
  p2_high_moi = metadata.filter(metadata["PassageNumber"] == passage_number)\
    .filter(metadata["MOI"] == moi)\
    .filter(metadata["MutagenicConditon"] == 'none')\
    .filter(metadata["ReplicateNumber"] == str(replicate_number))
  if p2_high_moi.count() == 0:
    print("No files found!")
  else:
    print("Files found:", p2_high_moi.count())
  p2_high_moi_files = p2_high_moi.select('SampleName').rdd.map(lambda r: "misp/CHIKV_CARIB/" + r.SampleName.lower() + "_del_sort.csv").collect()
  return p2_high_moi_files

# passage 2, 4, 5, 7, 9, 10, 12

for passage in (2, 4, 5, 7, 9, 10, 12):
  for replicate in (8, 9, 11, 12):
    files = find_files(passage, replicate)
    print(passage, replicate)
    print(files)
    
def get_total_read_counts():
  total_reads = spark.read.format("csv").load("misp/CHIKV_CARIB_total_reads.csv")
  total_reads.show()
  total_reads.printSchema()
  counts = total_reads.rdd.map(lambda r: (r._c0.lower(), int(r._c1))).collect()
  return dict(counts)
               
    
def mean(l):
  return __builtin__.sum(l)/float(len(l))


def get_del_frequency(files):
  
  df = spark.read.format("csv").option("header", "true").load(files)

  # Change some columns to ints
  df_num = df.withColumn("coveragei", df["coverage"].cast("int"))\
    .withColumn("starti", df["start position"].cast("int"))\
    .withColumn("sizei", df["size of event"].cast("int"))\
    .withColumn("readcounti", df["read count"].cast("int"))\
    .withColumn("filename", input_file_name()) # add filename
  
  # Only include deletions of size > 30
  df_num = df_num.filter(df_num["sizei"] > 30)

  # Only include read count over a certain size (selected by looking at the data)
  df_num = df_num.filter(df_num["readcounti"] > 5)
  
  df_num = df_num.groupBy("filename").agg(sum("readcounti").alias("sumrc"))
  
  r =  df_num.rdd.map(lambda r: (r.filename.split("/")[-1].replace("_del_sort.csv", ""), r.sumrc)).collect()

  return mean([count / float(c[filename]) for (filename, count) in r])


c = get_total_read_counts()
files=[]
for passage in (2, 4):
  for replicate in (8, 9):
    files = find_files(passage, replicate)
    print(passage, replicate)
    print(files)
    f = get_del_frequency(files)
    print(f)
    
