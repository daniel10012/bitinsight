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

uniq -w 26 address.csv > address2.csv

Regex

(?<![0-9])([0-9]|[1-8][0-9]|9[0-9]|[1-8][0-9]{2}|9[0-8][0-9]|99[0-9]|1000)(?![0-9])     0 < 1000
(?<![0-9])([0-9]|[1-8][0-9]|9[0-9]|[1-8][0-9]{2}|9[0-8][0-9]|99[0-9]|[1-8][0-9]{3}|9[0-8][0-9]{2}|99[0-8][0-9]|999[0-9]|10000)(?![0-9])
http://gamon.webfactional.com/regexnumericrangegenerator/

SELECT abs(hash(url)) % 10 as bucket, url, warc_filename, warc_record_offset, warc_record_length" +         " FROM ccindex WHERE subset = 'warc' AND content_languages='eng' " +         "AND (position('news' in url_host_name) != 0) df = spark.read.load(index_path)         df.createOrReplaceTempView("ccindex")          sqldf = spark.sql(args.query)


key_list = [] for file in range(20200101, 20200132):     key_list.append(f'gdelt-data/{file}.export.CSV') gdelt_log = spark.read.csv(     s3_files, schema, sep='\t' )


