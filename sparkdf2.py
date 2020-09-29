from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import explode, concat, col, lit, split, translate, row_number, arrays_zip, when, udf, sum, coalesce, collect_set
from pyspark.sql.types import *
from flagged_parsers.ofaclist import get_ofac_addresses
from pyspark.sql import Row
import pandas as pd
import CONFIG
#from graphframes import *

# Initiate Spark Session

spark = SparkSession\
        .builder\
        .appName("SparkDf")\
        .getOrCreate()

#spark_context = spark.sparkContext

# Import flagged addresses df

#OFAC
ofac_list = get_ofac_addresses()
ofac_df = spark.createDataFrame(Row(**x) for x in ofac_list).withColumn("address", explode("addresses")).withColumn("flagger", lit("OFAC")).withColumn("type", lit("nan"))
ofac_df = ofac_df.select("address", "flagger", "type", "abuser")
#ofac_df.show(truncate=False)

#bitcoinabuse

babuse_df = pd.read_csv("./flagged_data/records_forever.csv", encoding = "ISO-8859-1", sep=',', error_bad_lines=False, index_col=False, dtype=str)
babuse_df['flagger']='Bitcoinabuse'
babuse_df = babuse_df[["address", "flagger", "abuse_type_other", "abuser" ]]
babuse_df = spark.createDataFrame(babuse_df.astype(str), ["address", "flagger", "type", "abuser"])

#babuse_df.show()

# Consolidated flagged

flagged_df = ofac_df.union(babuse_df).dropDuplicates(["address"])

# Create blockchain schema

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


# Import blockchain in Dataframes

btc_df = spark.read.json(CONFIG.S3blocks, multiLine=True, schema=schema) \
       .withColumn("tx", explode("tx"))

btc_df.show()

# Get Vout dataframe

jtest_df = btc_df.select("tx", "time")          #.where(btc_df.height == 150000)
jtest_df = jtest_df.withColumn("vouts", jtest_df.tx.vout)
jtest_df = jtest_df.withColumn("vout", explode("vouts"))
jtest_df = jtest_df.withColumn("vout_id", concat(jtest_df.vout.n, lit("_"), jtest_df.tx.txid)).withColumn("value", jtest_df.vout.value).withColumn("addresses", jtest_df.vout.scriptPubKey.addresses)

#ad_vout_graph_df = jtest_df.select(jtest_df.tx.txid, jtest_df.addresses).show(truncate=False)

jtest_df = jtest_df.withColumn("address", explode("addresses")).withColumn("txid", jtest_df.tx.txid)
vout_df = jtest_df.select("txid", "time", "vout_id", "value", "address")
#vout_df.show(truncate=False)

# Get Vin dataframe

jtest_df = btc_df.select("tx")          #.where(btc_df.height == 150000)
jtest_df = jtest_df.withColumn("vin", jtest_df.tx.vin).withColumn("txid",jtest_df.tx.txid)
jtest_df = jtest_df.withColumn("vin", explode("vin"))
jtest_df = jtest_df.withColumn("vin_id", concat(jtest_df.vin.vout, lit("_"), jtest_df.vin.txid))
vin_df = jtest_df.drop("tx").drop("vin")
#vin_df.show(truncate=False)

# join vin vout dataframe
# joined_df = vout_df.join(vin_df, vout_df.txid == vin_df.txid, how='full')
# joined_df.show()

# Addresses dataframe and write to CSV
ad_df = vout_df.groupby("address").agg(sum("value").alias("total_value"))
ad_df = ad_df.select("address", "total_value")
address_df = ad_df.join(flagged_df, ad_df.address == flagged_df.address, "full").drop(flagged_df.address)

address_df = address_df.withColumn(":LABEL", when(address_df.flagger != "null", "Address;Flagged").otherwise("Address"))

address_df.show()

#address_df.where(address_df.address == "12QtD5BFwRsdNsAZY76UVE1xyCGNTojH9h").show(truncate=False)


address_df.write.csv(f"{CONFIG.S3csv}address_df", header=None)

# Tx dataframe

tx_df = btc_df.select("tx", "time")
tx_df = tx_df.withColumn("txid", tx_df.tx.txid).drop("tx")
tx_df = tx_df.select("txid", "time").withColumn(":LABEL", lit("Transaction"))
#tx_df.show()

tx_df.write.csv(f"{CONFIG.S3csv}tx_df", header=None)

# rel txad dataframe

rel_txad_df = vout_df.select("txid", "address").withColumn("type", lit("out"))

rel_txad_df.write.csv(f"{CONFIG.S3csv}rel_txad_df", header=None)
#rel_txad_df.show()


# rel adtx dataframe
rel_adtx_df = vin_df.join(vout_df, vin_df.vin_id == vout_df.vout_id)
rel_adtx_df = rel_adtx_df.select(vout_df.address, vin_df.txid).withColumn("type", lit("in"))
#rel_adtx_df.show(truncate=False)

rel_adtx_df.write.csv(f"{CONFIG.S3csv}rel_adtx_df", header=None)


# graphframes

# vertices = ad_df.select("address").distinct()
# vertices = vertices.withColumnRenamed("address", "id")
# vertices.show()
#
# edges = rel_adtx_df.select("txid", "address").withColumnRenamed("address", "address_in")
# edges = edges.join(rel_txad_df, edges.txid == rel_txad_df.txid).select("address_in", "address")
# edges = edges.withColumnRenamed("address_in", "src").withColumnRenamed("address", "dst")
# #edges = edges.groupBy("address").agg(collect_set('word').alias('words'))
# edges.show()
#
# g = GraphFrame(vertices, edges)
#
# g.inDegrees.show()
# spark_context.setCheckpointDir(CONFIG.S3graph)
# wallets = g.connectedComponents()
# wallets.show(100, truncate=False)
# #wallets.filter("id  = '1DryULtebgUdWNgt6tZBXdcgDDDPwSF3Z4'").show(truncate=False)
# #wallets.where("id  = '3NLSkSfYb4vdRvSNxgqUi2bxib58E7E9ip'").show(truncate=False)

