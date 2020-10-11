#!/bin/bash

BLOCKSTART=0
BLOCKEND=640000


for ((i=$BLOCKSTART; i<=$BLOCKEND;i+=1))
do

		echo "Sending block $i"

		BLOCKHEIGHT=$i

		BLOCKHASH=$(bitcoin-cli getblockhash $BLOCKHEIGHT)
    echo $BLOCKHASH

		FILENAME="block"${j}".json"

		bitcoin-cli getblock ${BLOCKHASH} 2 | aws s3 cp - s3://bitcoinwegmann/${FILENAME}
done



