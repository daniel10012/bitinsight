#!/bin/bash
# CONFIG.py

#for i in $(aws s3 ls s3://bucketname/ | grep 1400x1400); do aws s3 cp s3://bucketname/$i; done

BLOCKSTART=500000
BLOCKEND=501000


for ((i=$BLOCKSTART; i<=$BLOCKEND;i+=1))
do

		echo "Sending block $i to $TESTBUCKET"

		FILENAME="block"${i}".json"

		aws s3 cp s3://$FULLBUCKET/${FILENAME} s3://$TESTBUCKET/${FILENAME}
done



