import CONFIG
from rpc.extract import *
#https://intellipaat.com/community/4766/how-to-extract-all-used-hash160-addresses-from-bitcoin-blockchain
#to do
#manage case where type = pubkey (no address): https://bitcoin.stackexchange.com/questions/96865/why-does-vout-sometimes-not-have-address



from bitcoinrpc.authproxy import AuthServiceProxy

RPC_ADDRESS="127.0.0.1:8332"
RPC_USER=CONFIG.RPC_USER
RPC_PASSWORD=CONFIG.RPC_PASSWORD

def connect(address, user, password):
    return AuthServiceProxy("http://%s:%s@%s"%(user, password, address))

start_block = 200000
end_block = 200003

rpc = connect(RPC_ADDRESS, RPC_USER, RPC_PASSWORD)



# Get nodes

def get_nodes():

    outputs = []
    blocks = []
    transactions = []
    addresses = []

    for i in range(start_block, end_block+1):
        block_hash = rpc.getblockhash(i)

        # Outputs
        outps = extract_outputs(rpc, block_hash)
        outputs.extend(outps)

        # Blocks
        bl = extract_block(rpc, block_hash)
        blocks.append(bl.copy())

        # Transactions
        txs = extract_transactions(rpc, block_hash)
        transactions.extend(txs)

        # Addresses
        ads = extract_addresses(rpc, block_hash)
        addresses.extend(ads)


    return ({"outputs": outputs, "blocks": blocks, "transactions": transactions, "addresses": addresses})

#print(blocks)
#print(outputs)
#print(transactions)
#print(rels_blocks_tx)
#print(addresses)



# Create Dataframes




