#!/bin/bash

BLOCKSTART=0
BLOCKEND=200


for ((i=$BLOCKSTART; i<$BLOCKEND;i+=100))
do
	# Divide into chuncks to parallelize
	for ((j=i;j<i+100;j++))
	do
		echo "Sending block $j"

		BLOCKHEIGHT=$j

		BLOCKHASH=$(bitcoin-cli getblockhash $BLOCKHEIGHT)
    echo $BLOCKHASH

		FILENAME="block"${j}".json"

		bitcoin-cli getblock ${BLOCKHASH} 2 | aws s3 cp - s3://bitcoinwegmann/${FILENAME} &
	done
	wait
done

