#!/bin/bash
cat ../csvs/address_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/address.csv
cat ../csvs/tx_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/tx.csv
cat ../csvs/rel_adtx_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/rel_adtx.csv
cat ../csvs/rel_txad_df.csv/part* | sed -e '/^ *$/d' > clean_csvs/rel_txad.csv
