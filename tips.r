install.packages("sparklyr")

library(sparklyr)
library(dplyr)

# Copy data to HDFS by running this in Terminal access:
# hadoop fs -put data/tips.csv tips.csv

# Start a Spark session
sc <- spark_connect(master = "yarn-client")

# Create a Spark dataset from a CSV file
tips <- spark_read_csv(sc, "tips", "tips.csv")
tips

# Query to find if smokers tip more than non-smokers
# See https://spark.rstudio.com/dplyr/ for syntax
tips %>%
filter(total_bill > 5) %>%
group_by(smoker) %>%
summarise(mean_tip = mean(tip))

# End session
spark_disconnect(sc)