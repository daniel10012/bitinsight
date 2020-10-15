#!/bin/bash

cat address_df/part* | sed -e '/^ *$/d' > address.csv
cat tx_df/part* | sed -e '/^ *$/d' > tx.csv
cat rel_adtx_df/part* | sed -e '/^ *$/d' > rel_adtx.csv
cat rel_txad_df/part* | sed -e '/^ *$/d' > rel_txad.csv



awk '{gsub(/\"/,"")};1' part-00000-0838e035-cb11-4678-a3ca-4db234b656a7-c000.csv > ad.csv
sed 's/\\/ /g' ad.csv > ad2.csv

# Spark Cluster to S3
aws s3 cp address.csv s3://$bucket
aws s3 cp tx.csv s3://$bucket
aws s3 cp re_adtx.csv s3://$bucket
aws s3 cp rel_txad.csv s3://$bucket

# Cluster to neo4j

aws s3 cp s3://$bucket/address.csv localfolder
aws s3 cp s3://$bucket/tx.csv  localfolder
aws s3 cp s3://$bucket/re_adtx.csv localfolder
aws s3 cp s3://$bucket/rel_txad.csv localfolder


