# BitInsight

![Image of Cover](images/BitInsight.jpg)

Enabling detection of transaction links to blacklisted Bitcoins.
 
Compliance teams at exchanges and financial institutions can leverage this tool when onboarding new clients to comply with regulations.

A graph visualization of the Bitcoin blockchain enables easy tracking and pattern finding for suspicious transactions.

This solutions is offered for Bitcoin but would easily be transferable to other cryptocurrencies such as Ethereum or Litecoin.

# Table of Contents
1. [Motivation](README.md#Motivation)
2. [Dataset](README.md#Dataset)
3. [Pipeline](README.md#Pipeline)
4. [Installation](README.md#Installation)
5. [Visualization](README.md#Web-App)

## Motivation

Bitcoin is a new asset class that has been growing exponentially over the past 10 years.

Today its market cap stands of over 120 billion dollars, there are more than 50 millions active users and  over half a billion transactions have been recorded on chain.

Unfortunately, the relative anonymity it offers has given way to scams, illicit activities and money laundering.

This has attracted the scrutiny of the government leading to more stringent compliance regulation.

There are many Blockchain explorers out there based on relational databases, and while they are great to explore the content of blocks and transactions, they don't offer an easy way to link them together.

Our pipeline imports the Blockchain in a graph database that will allow easy tracking and pattern analysis of transactions.

Compliance teams are then able to easily query and visualize links to blacklisted bitcoins when onboarding new clients.
   

## Dataset

### Bitcoin Historical Data

The whole Blockchain up to now (640,000 Blocks as of Sep 2020) represents 300GB of raw data and 2.6TB when deserialized in JSON format.


### Flagged Addresses

Flagged addresses are obtained by scrapping the OFAC SDN list and using the API of Bitcoinabuse.com.

They are formatted in a unified schema and imported in a CSV of about 600,000 rows.



## Pipeline

![Image of Pipeline](images/pipeline.jpg)

### Data loading

#### Bitcoin
Bitcoin Core nodes download the blocks in raw format.
The data is deserialized in the form of JSON files by the way of JSON RPC calls.
The amount of data in each block is skewed: the last blocks contain many more transactions than the first blocks.
To allow fast parsing we split the work between 3 nodes, each processing respectively 400K, 200K and 50K blocks.
Each JSON block created is dropped into an S3 bucket.
See instructions to setup [Bitcoin Core](README.md#Bitcoin Core)
Run `./getblocks.sh` on each node with the appropriate start and end block values to deserialize Bitcoin block data into JSON and write into the dedicated AWS S3 bucket.


#### Flagged Addresses

**OFAC**

The scrapper parses the Specially Designated Nationals And Blocked Persons List updated regularly [here](https://www.treasury.gov/ofac/downloads/sdn.csv)

**Bitcoinabuse**

Download the complete list of self declared scam Bitcoin addresses with the [API](https://www.bitcoinabuse.com/api-docs)
Put the resulting CSV in the `flagged` folder.

**Full Flagged List**

Run `python3 flagged_list.py` to get the full aggregated Flagged list


### Transformation

Once the [spark cluster](README.md#Spark) is running, run `.workflow.sh`  on the master instance.

This will extract the Bitcoin data and the Flagged data and ingest them in dataframes for processing.

The data will be written in CSV form in 4 different folders in S3:
- address: addresses with a label `FLAGGED` if they have been marked
- tx: transactions with transaction hash and time of execution
- rel_ad_tx: `IN` links between addresses and transaction, meaning an address is a sender that transaction
- rel_tx_ad: `OUT` links showing transactions sending bitcoin to addresses

Those files will be ingested in our [Neo4j](README.md#Neo4j) graph database for querying.


## Installation

- 3 Bitcoin Core nodes on M5 large instances
- Spark cluster with 1 m5 large driver and 3 r5.4xlarge instances
- 1 m5 large instance for the Neo4j database


### Bitcoin Core

Installing Bitcoin Core will synchronize and download the blockchain from peers on the network in serialized blk.*dat files.

When the files are up to data we will be able to query them with JSON RPC calls and get JSON files in return.

The raw data is around 300GB (sep2020) so we will need 3 m5 large instances with each an EBS of 150GB attached.

On each instance type `wget https://bitcoin.org/bin/bitcoin-core-0.20.0/bitcoin-0.20.0-x86_64-linux-gnu.tar.gz` to install Bitcoin core.

Then unpack it with `tar -xvf bitcoin-0.20.0-x86_64-linux-gnu.tar.gz` and remove the `.tar.gz` file

Edit and source your `.bashrc` to include `alias bitcoind="/home/ubuntu/bitcoin-0.20.0/bin/bitcoind"` and `alias bitcoin-cli="/home/ubuntu/bitcoin-0.20.0/bin/bitcoin-cli"`

Launch the bitcoin daemon with the command `bitcoind -daemon -txindex=1`

Blocks are now downloading and will appear in `~/.bitcoin/blocks`

You can check the progress by typing `bitcoin-cli getblockcount` to get the number of blocks downloaded.

Once it is complete, you can run `getblocks.sh` to make JSON RPC calls to the node and send the deserialized blocks in JSON format to the S3 bucket (1 file per block).

This script leverages multithreading to speed up the process.



### Spark

Setup a Spark cluster with 1 m5 large driver node and 3 r5.4xlarge workers, each with 100GB root volume attached.

Run `./worklfow` on the driver to start parsing the Bitcoin data and writing to csvs

### Neo4j

Install Neo4j on a m5 large instance with 300GB disk.

import the csv in /import and concatenate

run `neo4j admin import bla bla`

## Visualization

We can now visualize and query the bitcoin blockchain.

### Find link to blacklisted address
CYPHER blabla
publicip/7473