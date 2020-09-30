#!/bin/bash
cat ../csvs/address_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/address.csv
cat ../csvs/tx_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/tx.csv
cat ../csvs/rel_adtx_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/rel_adtx.csv
cat ../csvs/rel_txad_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/rel_txad.csv


cat address_df/part* | sed -e '/^ *$/d' > address.csv
cat tx_df/part* | sed -e '/^ *$/d' > tx.csv
cat rel_adtx_df/part* | sed -e '/^ *$/d' > rel_adtx.csv
cat rel_txad_df/part* | sed -e '/^ *$/d' > rel_txad.csv



awk '{gsub(/\"/,"")};1' part-00000-0838e035-cb11-4678-a3ca-4db234b656a7-c000.csv > ad.csv
sed 's/\\/ /g' ad.csv > ad2.csv
