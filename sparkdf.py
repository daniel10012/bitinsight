from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import explode, concat, col, lit, split, translate, row_number, arrays_zip, when, udf, sum
from pyspark.sql.types import *
from flagged_parsers.ofaclist import get_ofac_addresses
from pyspark.sql import Row
import pandas as pd


# Initiate Spark Session

spark = SparkSession\
        .builder\
        .appName("SparkDf")\
        .getOrCreate()


# Import flagged addresses df

#OFAC
ofac_list = get_ofac_addresses()
ofac_df = spark.createDataFrame(Row(**x) for x in ofac_list).withColumn("address", explode("addresses")).withColumn("flagger", lit("OFAC"))
ofac_df = ofac_df.select("address", "flagger", "abuser")
ofac_df.show()

#bitcoinabuse

babuse_df = pd.read_csv("./flagged_data/records_forever.csv", encoding = "ISO-8859-1", sep=',', error_bad_lines=False, index_col=False, dtype=str)
babuse_df['flagger']='Bitcoinabuse'
babuse_df = babuse_df[["address", "flagger", "abuse_type_other", "abuser" ]]
babuse_df = spark.createDataFrame(babuse_df.astype(str))

babuse_df.show()

# Consolidated flagged








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

btc_df = spark.read.json("./json", multiLine=True, schema=schema) \
       .withColumn("tx", explode("tx"))

#btc_df.show()

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












