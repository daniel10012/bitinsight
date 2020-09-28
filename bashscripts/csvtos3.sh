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
