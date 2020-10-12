#!/bin/bash

BLOCKSTART=0
BLOCKEND=640000


for ((i=$BLOCKSTART; i<$BLOCKEND;i+=100))
do

	for ((j=i;j<i+100;j++))
	do
		echo "Sending block $j"

		BLOCKHEIGHT=$j

		BLOCKHASH=$(bitcoin-cli getblockhash $BLOCKHEIGHT)
    echo $BLOCKHASH

		FILENAME="block"${j}".json"

		bitcoin-cli getblock ${BLOCKHASH} 2 | aws s3 cp - s3://CONFIG.s3blocks/${FILENAME} &
	done
	wait
done

