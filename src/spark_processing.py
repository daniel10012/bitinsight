import sys
from src import CONFIG
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, concat, lit, when, broadcast, from_unixtime
from pyspark.sql.types import *
import awswrangler as wr
from src.utils import bitcoin_files_names



def parse_bitcoin():
    """
    Parsing function to get the full blockchain and flagged addresses data.
    Extracts addresses, transaction and flagging information.
    Writes the data to formatted csvs.
    """

    # set up spark session
    spark = SparkSession \
        .builder \
        .appName("SparkDf") \
        .getOrCreate()


    # get batch list of bitcoin files

    list_files = bitcoin_files_names(sys.argv[5])


    # Import flagged addresses

    flagged_pf = wr.s3.read_parquet(path=f"{CONFIG.S3flagged}/flagged_pf.parquet")
    flagged_df = spark.createDataFrame(flagged_pf.astype(str))


    # Create blockchain schema

    schema = StructType([
        StructField('hash', StringType(), True),
        StructField('height', LongType(), True),
        StructField('time', LongType(), True),
        StructField('tx', ArrayType(
            StructType([
                StructField('hash', StringType(), True),
                StructField('txid', StringType(), True),
                StructField('vin', ArrayType(
                    StructType([
                        StructField('scriptSig', StructType([
                            StructField('asm', StringType(), True),
                            StructField('hex', StringType(), True),
                        ]), True),
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
                            StructField('type', StringType(), True)
                        ]), True),
                        StructField('value', DoubleType(), True)
                    ])
                ), True),
                StructField('weight', LongType(), True)
            ])
        ), True)
    ])


    # Import blockchain in Dataframes

    btc_df = spark.read.json(list_files, multiLine=True, schema=schema) \
           .withColumn("tx", explode("tx")).coalesce(140)


    # Get Vout dataframe

    json_df = btc_df.select("tx", "time")
    json_df = json_df.withColumn("vouts", json_df.tx.vout)
    json_df = json_df.withColumn("vout", explode("vouts"))
    json_df = json_df.withColumn("vout_id", concat(json_df.vout.n, lit("_"), json_df.tx.txid)) \
              .withColumn("value", json_df.vout.value).withColumn("addresses", json_df.vout.scriptPubKey.addresses)
    json_df = json_df.withColumn("address", explode("addresses")).withColumn("txid", json_df.tx.txid)
    vout_df = json_df.select("txid", "time", "vout_id", "value", "address")


    # Write vout_df to parquet

    vout_df.write.mode('append').parquet(f"{CONFIG.S3vout}/vout_df.parquet")

    # Reread from parquet to get full vout history in batches

    vout_df_all = spark.read.parquet(f"{CONFIG.S3vout}/vout_df.parquet")


    # Get Vin dataframe

    json_df2 = btc_df.select("tx")
    json_df2 = json_df2.withColumn("vin", json_df2.tx.vin).withColumn("txid",json_df2.tx.txid)
    json_df2 = json_df2.withColumn("vin", explode("vin"))
    json_df2 = json_df2.withColumn("vin_id", concat(json_df2.vin.vout, lit("_"), json_df2.vin.txid))
    vin_df = json_df2.drop("tx").drop("vin")


    # Create address dataframe and write to CSV

    ad_df = vout_df.select("address").distinct()
    address_df = ad_df.join(broadcast(flagged_df), ad_df.address == flagged_df.address, "left").drop(flagged_df.address)
    address_df = address_df.withColumn(":LABEL", when(address_df.flagger != "null", "Address;Flagged").otherwise("Address"))

    address_df.write.csv(f"{CONFIG.S3csv}/address_df", header=None, mode="append")


    # Transactions dataframe

    tx_df = btc_df.select("tx", "time").withColumn('stringtime', from_unixtime(btc_df.time,'yyyy-MM-dd HH:mm:ss'))
    tx_df = tx_df.withColumn("txid", tx_df.tx.txid).drop("tx").drop("time")
    tx_df = tx_df.withColumnRenamed("stringtime", "time")
    tx_df = tx_df.select("txid", "time").withColumn(":LABEL", lit("Transaction"))

    tx_df.write.csv(f"{CONFIG.S3csv}/tx_df", header=None, mode="append")


    # Relations between transactions and addresses dataframe

    rel_txad_df = vout_df.select("txid", "address").withColumn("type", lit("out"))

    rel_txad_df.write.csv(f"{CONFIG.S3csv}/rel_txad_df", header=None, mode="append")


    # Relations between addresses and transactions dataframe

    rel_adtx_df = vin_df.join(vout_df_all, vin_df.vin_id == vout_df_all.vout_id)
    rel_adtx_df = rel_adtx_df.select(vout_df_all.address, vin_df.txid).withColumn("type", lit("in"))

    rel_adtx_df.write.csv(f"{CONFIG.S3csv}/rel_adtx_df", header=None, mode="append")


    # stop spark session

    spark.stop()



if __name__ == "__main__":
    parse_bitcoin()