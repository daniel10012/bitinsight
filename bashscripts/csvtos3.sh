# Spark Cluster to S3
aws s3 cp address.csv s3://$bucket
aws s3 cp tx.csv s3://$bucket
aws s3 cp re_adtx.csv s3://$bucket
aws s3 cp rel_txad.csv s3://$bucket

# Cluster to neo4j

aws s3 cp s3://$bucket/address.csv localfolder
aws s3 cp s3://$bucket/tx.csv  localfolder
aws s3 cp s3://$bucket/re_adtx.csv localfolder
aws s3 cp s3://$bucket/rel_txad.csv localfolder



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