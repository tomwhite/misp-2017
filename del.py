from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.functions import max
import matplotlib.pyplot as plt
  
# Start a Spark session
spark = SparkSession\
  .builder\
  .appName("misp-del")\
  .getOrCreate()
  
# Get all the files for a given passage and high MOI
def find_files(passage_number, moi='high'):
  # Read metadata into an RDD so we can query it
  metadata = spark.read.format("csv").option("header", "true").load("misp/CHIKV_CARIB_metadata.csv")
  metadata.count()

  # Find files for given criteria
  p2_high_moi = metadata.filter(metadata["PassageNumber"] == passage_number)\
    .filter(metadata["MOI"] == moi)\
    .filter(metadata["MutagenicConditon"] == 'none')\
    .filter(metadata["ReplicateNumber"].isin("8", "9", "11", "12"))\
    .filter(metadata["SampleName"].like("%C1%"))
  if p2_high_moi.count() == 0:
    print("No files found!")
  else:
    print("Files found:", p2_high_moi.count())
  p2_high_moi_files = p2_high_moi.select('SampleName').rdd.map(lambda r: "misp/CHIKV_CARIB/" + r.SampleName.lower() + "_del_sort.csv").collect()
  return p2_high_moi_files

def get_aggregated_read_counts_by_del_start(files):
  # Read CSVs into a dataframe
  #files = "misp/CHIKV_CARIB_DEL/*_p2_*.csv"

  df = spark.read.format("csv").option("header", "true").load(files)

  # Change some columns to ints
  df_num = df.withColumn("coveragei", df["coverage"].cast("int")).withColumn("starti", df["start position"].cast("int")).withColumn("sizei", df["size of event"].cast("int")).withColumn("readcounti", df["read count"].cast("int"))

  # Only include deletions of size > 30
  df_num = df_num.filter(df_num["sizei"] > 30)

  # Only include read count over a certain size (selected by looking at the data)
  df_num = df_num.filter(df_num["readcounti"] > 5)

  # What's the max read count?
  max_read_count = df_num.agg(max("readcounti")).collect()[0]
  max_read_count

  # Get aggregrated read count by position
  df_grouped = df_num.groupBy("starti").agg(sum("readcounti").alias('agg_read_count')).orderBy("starti")
  # Get counts of del by position
  #df_grouped = df_num.groupBy("starti").count().orderBy("starti")
  
  return df_grouped
  

# Gene seq: https://www.ncbi.nlm.nih.gov/nuccore/LN898112.1

genes = {
  "nsP1": (77, 1681),
  "nsP2": (1682, 4072),
  "nsP3": (4073, 5653),
  "nsP4": (5654, 7486),
  "C": (7555, 8337),
  "E3": (8338, 8529),
  "E2": (8530, 9798),
  "6K": (9799, 9981),
  "E1": (9982, 11298),
}

passages = [2, 4, 5, 7, 9, 10, 12]

def show_chart(df_grouped, title):
  # Convert to pandas and show a bar chart with matplotlib
  pdf=df_grouped.toPandas()
  
  plt.scatter(pdf['starti'], pdf['agg_read_count'])

  x1,x2,y1,y2 = plt.axis()
  gene_y_pos=-y2/20
  plt.axis((x1,x2,2 * gene_y_pos,y2))

  offset = 100
  index=0
  for (gene, (start, end)) in genes.items():
    plt.axvline(x=start, color='r', linestyle='dashed')
    plt.axvline(x=end, color='r', linestyle='dashed')
    plt.text(start + offset, (index % 2 + 1) * gene_y_pos, gene, fontsize=8)
    index += 1
      
  plt.title('Deletion frequency by position ' + title)
  plt.xlabel('DEL start')
  plt.ylabel('Aggregate read count')
  
  
  plt.show()
  
for passage in passages:
  files = find_files(passage)
  df = get_aggregated_read_counts_by_del_start(files)
  show_chart(df, "(Passage %s)" % passage)

p12_files = find_files(12)
p12_df = get_aggregated_read_counts_by_del_start(p12_files)
show_chart(p12_df, "(Passage 12)")
# Notice deletion start positions disappear for the structural proteins

# Find distribution of del lengths
def get_del_dist(files):
  df = spark.read.format("csv").option("header", "true").load(files)
  # Change some columns to ints
  df_num = df.withColumn("coveragei", df["coverage"].cast("int")).withColumn("starti", df["start position"].cast("int")).withColumn("sizei", df["size of event"].cast("int")).withColumn("readcounti", df["read count"].cast("int"))
  
  # Only include deletions of size > 30
  df_num = df_num.filter(df_num["sizei"] > 30)

  return df_num
  
def show_del_len_hist(df):
  pdf=df.toPandas()
  plt.hist(pdf['sizei'], bins=30)

p2_del_dist = get_del_dist(p2_files)
show_del_len_hist(p2_del_dist)

p12_del_dist = get_del_dist(p12_files)
show_del_len_hist(p12_del_dist)
  
# TODO: which part of genome has a DEL in each passage?