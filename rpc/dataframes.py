from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as sf
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, BooleanType, LongType
from rpc.nodes import get_nodes
import time
start_time = time.time()

# Get nodes
nodes = get_nodes()
outputs = nodes["outputs"]
blocks = nodes["blocks"]
transactions = nodes["transactions"]
addresses = nodes["addresses"]



appName = "BTC Spark"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# Outputs Dataframe
outputs_df = spark.createDataFrame(Row(**x) for x in outputs)
outputs_df = outputs_df.withColumn('outputId',  sf.concat(sf.col('index'),sf.lit('_'), sf.col('txid')))   #create outputId
outputs_df = outputs_df.select('outputId', 'txid', 'index', 'value', 'script_asm', 'script_hex', 'type')
# print(outputs_df.schema)
outputs_df.show()

# Blocks Dataframe
# blocks_df = spark.createDataFrame(Row(**x) for x in blocks)
# print(blocks_df.schema)
#blocks_df.show()

# Transactions Dataframe
#transactions_df = spark.createDataFrame(Row(**x) for x in transactions)
schema = StructType([StructField("block_hash",StringType(), False),StructField("coinbase",BooleanType(), False),StructField("time",LongType(), False),StructField("txid",StringType(), False),StructField("vouts",ArrayType(StringType(), True), True),StructField("vins",ArrayType(StringType(), True),True)])
transactions_df = spark.createDataFrame(transactions, schema)
transactions_df = transactions_df.withColumn("vout", sf.explode_outer(transactions_df.vouts)).withColumn("vin", sf.explode_outer(transactions_df.vins))
transactions_df = transactions_df.select("txid", "coinbase", "time", "vout", "vin", "block_hash")
#print(transactions_df.schema)
#transactions_df.show()

# Addresses
addresses_inputs_df = spark.createDataFrame(Row(**x) for x in addresses)
addresses_inputs_df = addresses_inputs_df.withColumn("addresses", sf.explode(addresses_inputs_df.addresses))
addresses_df = addresses_inputs_df.select("addresses").distinct()
#print(addresses_df.schema)



# rel_blocks
# cols = ['hash','prevhash']
# rel_blocks = blocks_df.select(*cols)

# rel_blocks_tx
# cols = ['block_hash','txid']
# rel_blocks_tx = transactions_df.select(*cols)
# #rel_blocks_tx.show()

# rel tx_outputs
rel_tx_outputs = outputs_df.select('txid', 'outputId')
#rel_tx_outputs.show()

# rel_inputs_tx
rel_inputs_tx = transactions_df.select("vin", "txid")
#rel_inputs_tx.show()

# rel_ao
rel_ao_df = addresses_inputs_df
#rel_ao_df.show()


# scams dataframe
scams_df = spark.read.option("header",True) \
     .csv("records_forever.csv")
#scams_df.show()






# Create CSVs

# Outputs Dataframe
outputs_df.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/outputs_df.csv')
# Blocks Dataframe
# blocks_df.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/blocks_df.csv')
# Transactions
transactions_df.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/transactions_df.csv')
# Addresses
addresses_df.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/addresses_df.csv')
# rel_blocks
# rel_blocks.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/rel_blocks.csv')
# rel_blocks_tx
#rel_blocks_tx.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/rel_blocks_tx.csv')
# rel tx_outputs
rel_tx_outputs.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/rel_tx_outputs.csv')
# rel_inputs_tx
rel_inputs_tx.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/rel_inputs_tx.csv')
# rel_ao
rel_ao_df.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('./csvs/rel_ao.csv')

print("--- %s seconds ---" % (time.time() - start_time))

# postgres to do
# mode = "overwrite"
# url = 'jdbc:postgresql://localhost/test_db'
# properties = {"user": "postgres", "password": "Sunrise2016$", "driver": "org.postgresql.Driver"}
# transactions_df.write.jdbc(url=url, table="transactions", mode=mode, properties=properties)