from __future__ import print_function

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.types import *


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import explode, concat, col, lit, split, translate, row_number, arrays_zip, when, udf, sum
from pyspark.sql.types import *
from pyspark.sql.window import Window


spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

# def array_of_arrays_to_string(x):
#     """
#     UDF function
#     Parses single and multisig addresses
#     """
#     result = []
#     for val in x:
#         if val is not None:
#             if len(val) == 1:
#                 result.append(str(val[0]))
#             else:
#                 multisig = " | ".join([str(x) for x in val])
#                 result.append(multisig)
#     return result

# create custom schema for blockchain data
schema = StructType([
    StructField('bits', StringType(), True),
    StructField('chainwork', StringType(), True),
    StructField('confirmations', LongType(), True),
    StructField('difficulty', DoubleType(), True),
    StructField('hash', StringType(), True),
    StructField('height', LongType(), True),
    StructField('mediantime', LongType(), True),
    StructField('merkleroot', StringType(), True),
    StructField('nTx', LongType(), True),
    StructField('nextblockhash', StringType(), True),
    StructField('nonce', LongType(), True),
    StructField('previousblockhash', StringType(), True),
    StructField('size', LongType(), True),
    StructField('strippedsize', LongType(), True),
    StructField('time', LongType(), True),
    StructField('tx', ArrayType(
        StructType([
            StructField('hash', StringType(), True),
            StructField('hex', StringType(), True),
            StructField('locktime', LongType(), True),
            StructField('size', LongType(), True),
            StructField('txid', StringType(), True),
            StructField('version', LongType(), True),
            StructField('vin', ArrayType(
                StructType([
                    StructField('coinbase', StringType(), True),
                    StructField('scriptSig', StructType([
                        StructField('asm', StringType(), True),
                        StructField('hex', StringType(), True),
                    ]), True),
                    StructField('sequence', LongType(), True),
                    StructField('txid', StringType(), True),
                    StructField('vout', LongType(), True),
                    StructField('txinwitness', ArrayType(
                        StringType()
                    ), True)
                ])
            ), True),
            StructField('vout', ArrayType(
                StructType([
                    StructField('n', LongType(), True),
                    StructField('scriptPubKey', StructType([
                        StructField('addresses', ArrayType(
                            StringType()
                        ), True),
                        StructField('asm', StringType(), True),
                        StructField('hex', StringType(), True),
                        StructField('reqSigs', LongType(), True),
                        StructField('type', StringType(), True)
                    ]), True),
                    StructField('value', DoubleType(), True)
                ])
            ), True),
            StructField('vsize', LongType(), True),
            StructField('weight', LongType(), True)
        ])
    ), True),
    StructField('version', LongType(), True),
    StructField('versionHex', StringType(), True),
    StructField('weight', LongType(), True)
])

# ---READ IN JSON DATA FROM S3 AND PROCESS---



btc_df = spark.read.json("./json", multiLine=True, schema=schema) \
       .withColumn("tx", explode("tx"))

btc_df.show()

# Get Vout dataframe

jtest_df = btc_df.select("tx", "time")          #.where(btc_df.height == 150000)
jtest_df = jtest_df.withColumn("vouts", jtest_df.tx.vout)
jtest_df = jtest_df.withColumn("vout", explode("vouts"))
jtest_df = jtest_df.withColumn("vout_id", concat(jtest_df.vout.n, lit("_"), jtest_df.tx.txid)).withColumn("value", jtest_df.vout.value).withColumn("addresses", jtest_df.vout.scriptPubKey.addresses)
jtest_df = jtest_df.withColumn("address", explode("addresses")).withColumn("txid", jtest_df.tx.txid)
vout_df = jtest_df.select("txid", "time", "vout_id", "value", "address")
vout_df.show(truncate=False)

# Get Vin dataframe

jtest_df = btc_df.select("tx")          #.where(btc_df.height == 150000)
jtest_df = jtest_df.withColumn("vin", jtest_df.tx.vin).withColumn("txid",jtest_df.tx.txid)
jtest_df = jtest_df.withColumn("vin", explode("vin"))
jtest_df = jtest_df.withColumn("vin_id", concat(jtest_df.vin.vout, lit("_"), jtest_df.vin.txid))
vin_df = jtest_df.drop("tx").drop("vin")
vin_df.show(truncate=False)

# join vin vout dataframe
#joined_df = vout_df.join(vin_df, vout_df.txid == vin_df.txid, how='full')
#joined_df.show()

# Addresses dataframe and write to CSV
address_df = vout_df.groupby("address").agg(sum("value").alias("total_value"))
address_df = address_df.select("address", "total_value").withColumn(":LABEL", lit("Address"))
#address_df.show()

address_df.repartition(1).write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/address_df.csv')


# Tx dataframe

tx_df = btc_df.select("tx", "time")
tx_df = tx_df.withColumn("txid", tx_df.tx.txid).drop("tx")
tx_df = tx_df.select("txid", "time").withColumn(":LABEL", lit("Transaction"))
#tx_df.show()

tx_df.repartition(1).write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/tx_df.csv')

# rel txad dataframe

rel_txad_df = vout_df.select("txid", "address").withColumn("type", lit("out"))
rel_txad_df.repartition(1).write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/rel_txad_df.csv')
#rel_txad_df.show()


# rel adtx dataframe
rel_adtx_df = vin_df.join(vout_df, vin_df.vin_id == vout_df.vout_id)
rel_adtx_df = rel_adtx_df.select(vout_df.address, vin_df.txid).withColumn("type", lit("in"))
rel_adtx_df.show(truncate=False)

rel_adtx_df.repartition(1).write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/rel_adtx_df.csv')




# tx_df = json_df.select("hash", "time", "tx")
# #
# # tx_df = tx_df.withColumn("tx_id", )
# #tx_df.show(truncate=False)
#
# print(tx_df.select("tx").dtypes)

# tx_test_df = tx_df.select(element_at(col("tx"), 0))
#
# tx_test_df.show()

#element_at(col('arr'), -1).alias('1st_from_end')











# # prepare UDF function for processing
# convert_udf = udf(lambda x: array_of_arrays_to_string(x), StringType())
# # process DataFrame to return specific columns
#
# tx_df = json_df.withColumn("txid_pre", json_df.tx.txid)\
#         .withColumn("txid", when((json_df.tx.txid == 'e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468') | (json_df.tx.txid == 'd5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599'), concat(col("txid_pre"), lit("-"), col("height"))).otherwise(json_df.tx.txid))\
#         .withColumn("vin_coinbase", concat_ws(",", json_df.tx.vin.coinbase))\
#         .withColumn("vin_txid", concat_ws(",", json_df.tx.vin.txid))\
#         .withColumn("vin_vout", concat_ws(",", json_df.tx.vin.vout))\
#         .withColumn("vout_value", concat_ws(",", json_df.tx.vout.value))\
#         .withColumn("vout_n", concat_ws(",", json_df.tx.vout.n))\
#         .withColumn("vout_addresses_pre", json_df.tx.vout.scriptPubKey.addresses)\
#         .withColumn("vout_addresses", convert_udf("vout_addresses_pre"))\
#         .drop("tx")\
#         .drop("vout_addresses_pre")\
#         .drop("nonce")\
#         .drop("txid_pre")
#
# # select priority columns, convert array columns, and zip vin and vout fields
# clean_df = tx_df.withColumn("vin_txid_arr", split(col("vin_txid"), ",\s*")) \
#     .withColumn("vin_vout_arr", split(col("vin_vout"), ",\s*")) \
#     .withColumn("vin_txid_vout_zip", arrays_zip("vin_txid_arr", "vin_vout_arr")) \
#     .withColumn("vout_value_arr", split(col("vout_value"), ",\s*")) \
#     .withColumn("vout_n_arr", split(col("vout_n"), ",\s*")) \
#     .withColumn("vout_addresses_arr", split(col("vout_addresses"), ",\s*")) \
#     .withColumn("vout_value_n_addr_zip", arrays_zip("vout_value_arr", "vout_n_arr", "vout_addresses_arr"))
# # display_df(clean_df)
#
# # # create left side DataFrame
# vin_cols = ['txid', 'height', 'time', 'ntx', 'vin_coinbase', 'vin_txid_vout_zip']
# vin_df = clean_df.select(vin_cols) \
#     .withColumn("vin_txid_vout_tup", explode("vin_txid_vout_zip")) \
#     .withColumn("vin_txid", col("vin_txid_vout_tup").vin_txid_arr) \
#     .withColumn("vin_vout", col("vin_txid_vout_tup").vin_vout_arr) \
#     .drop("vin_txid_vout_zip") \
#     .drop("vin_txid_vout_tup") \
#     .withColumn("left_key", concat(col("vin_txid"), lit("-"), col("vin_vout")))
# # display_df(vin_df)
#
# # create right side DataFrame
# vout_cols = ['txid', 'vout_value_n_addr_zip']
# vout_df = clean_df.select(vout_cols) \
#     .withColumn("vout_value_n_addr_tup", explode("vout_value_n_addr_zip")) \
#     .withColumn("vout_value", col("vout_value_n_addr_tup").vout_value_arr) \
#     .withColumn("vout_n", col("vout_value_n_addr_tup").vout_n_arr) \
#     .withColumn("vout_addr_pre", col("vout_value_n_addr_tup").vout_addresses_arr) \
#     .withColumn("vout_addr", translate(col("vout_addr_pre"), '[]', '')) \
#     .drop("vout_value_n_addr_zip") \
#     .drop("vout_value_n_addr_tup") \
#     .drop("vout_addr_pre") \
#     .withColumnRenamed("txid", "txid2") \
#     .withColumn("right_key", concat(col("txid2"), lit("-"), col("vout_n"))) \
#     .drop("txid2")
# # display_df(vout_df)
#
# # join DataFrames
# join_df = vin_df.join(vout_df, vin_df.left_key == vout_df.right_key, 'left') \
#     .drop("left_key") \
#     .drop("right_key")
#
# # create temporary table for GraphFrames
# join_df.registerTempTable("join_result")
#
# # create vertices DataFrame
# vertices = spark.sql("SELECT DISTINCT(vout_addr) FROM join_result").withColumnRenamed("vout_addr", "id")
#
# # generate DataFrame with single address connection for all addresses in a given txid group
# w = Window.partitionBy("txid").orderBy("vout_addr")
# first_by_txid_df = join_df.withColumn("rn", row_number().over(w)).where(col("rn") == 1) \
#     .withColumnRenamed("txid", "txid2") \
#     .withColumnRenamed("vout_addr", "vout_addr_first") \
#     .drop("rn") \
#     .drop("height")
#
# #first_by_txid_df.show(100)
#
# # join DataFrames
# interim_df = join_df.join(first_by_txid_df, join_df.txid == first_by_txid_df.txid2, 'left')
#
# # create edges DataFrame
# edges = interim_df.select("vout_addr", "vout_addr_first") \
#     .withColumnRenamed("vout_addr", "src") \
#     .withColumnRenamed("vout_addr_first", "dst") \
#     .na.drop()
#
# #vertices.show()
# edges.show(1000)