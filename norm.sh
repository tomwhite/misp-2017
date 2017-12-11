#!/usr/bin/env bash

# ./norm.sh MISP/CHIKV_CARIB_bbmap_depth MISP/CHIKV_CARIB_total_reads.csv
# ./norm.sh MISP/CHIKV_IOL_bbmap_depth MISP/CHIKV_IOL_total_reads.csv
# ./norm.sh MISP/Zika_bbmap_depth MISP/Zika_total_reads.csv

input=$1
output=$2
rm -f $output

# For each file sum the coverage from the bbmap_depth files and divide by 150, the
# typical read size to get an approximate count of the total reads for the sample.
# We will update these approximate numbers by counting the number of reads in the BAM
# files later, when we have access to them.

for file in $(ls $input/*.txt); do
  sample=$(basename $(basename $file) _bbmap_depth.txt)
  total_reads=$(awk '{sum+=$3} END {print int(sum/150)}' $file)
  echo "$sample,$total_reads" >> $output
done