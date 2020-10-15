#!/bin/bash

# S3 to Neo4j

aws s3 cp s3://$CONFIG.S3csvs/address_df [neo4j public DNS]/var/lib/imports
aws s3 cp s3://$CONFIG.S3csvs/tx_df  [neo4j public DNS]/var/lib/imports
aws s3 cp s3://$CONFIG.S3csvs/rel_adtx_df [neo4j public DNS]/var/lib/imports
aws s3 cp s3://$CONFIG.S3csvs/rel_txad_df [neo4j public DNS]/var/lib/imports


# Concatenate CSVs

cat address_df/part* | sed -e '/^ *$/d' > address.csv
cat tx_df/part* | sed -e '/^ *$/d' > tx.csv
cat rel_adtx_df/part* | sed -e '/^ *$/d' > rel_adtx.csv
cat rel_txad_df/part* | sed -e '/^ *$/d' > rel_txad.csv







