#!/bin/bash
cat ~/Documents/Project/bitinsight/csvs/address_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/address.csv
cat ~/Documents/Project/bitinsight/csvs/tx_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/tx.csv
cat ~/Documents/Project/bitinsight/csvs/rel_adtx_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/rel_adtx.csv
cat ~/Documents/Project/bitinsight/csvs/rel_txad_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/rel_txad.csv
