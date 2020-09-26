from decimal import *

# Outputs
def extract_outputs(rpc, block_hash):

    block = rpc.getblock(block_hash)
    output = {}
    outputs = []
    for tx in block[u'tx']:
        raw_tx = rpc.getrawtransaction(tx, True)

        for vout in raw_tx["vout"]:
            output["txid"] = raw_tx["txid"]
            output["index"] = vout["n"]
            output["value"] = Decimal(vout["value"])
            output["script_asm"] = vout["scriptPubKey"]["asm"]
            output["script_hex"] = vout["scriptPubKey"]["hex"]
            output["type"] = vout["scriptPubKey"]["type"]

            outputs.append(output.copy())

    return outputs

# Blocks
def extract_block(rpc, block_hash):

    block = rpc.getblock(block_hash)

    bl = {}
    bl["hash"] = block_hash
    bl["size"] = block["size"]
    bl["height"] = block["height"]
    bl["time"] = block["time"]
    bl["ntx"] = len(block["tx"])
    bl["prevhash"] = block["previousblockhash"]
    bl["difficulty"] = block["difficulty"]

    return bl

# Transactions

def extract_transactions(rpc, block_hash):

    block = rpc.getblock(block_hash)

    t = {}
    txs = []
    for tx in block[u'tx']:
        raw_tx = rpc.getrawtransaction(tx, True)
        t["txid"] = raw_tx["txid"]
        t["block_hash"] = block_hash
        t["time"] = raw_tx["time"]
        t["coinbase"] = True if ("coinbase" in raw_tx["vin"][0]) else False
        if t["coinbase"] == False:
            t["vins"] = [str(vin["vout"]) + "_" + vin["txid"] for vin in raw_tx["vin"]]
        else:
            t["vins"] = None
        t["vouts"] = [str(vout["n"]) + "_" + raw_tx["txid"] for vout in raw_tx["vout"]]
        txs.append(t.copy())

    return txs

# Addresses

def extract_addresses(rpc, block_hash):

    block = rpc.getblock(block_hash)
    address = {}
    addresses = []

    for tx in block[u'tx']:
        raw_tx = rpc.getrawtransaction(tx, True)

        if not 'vout' in raw_tx:
            break

        for vout in raw_tx[u'vout']:

            if not "scriptPubKey" in vout:
                break

            if vout["scriptPubKey"]["type"] == "nulldata":
                # arbitrary data
                break

            elif 'addresses' in vout['scriptPubKey']:
                #print(vout['scriptPubKey']['addresses'])
                address["addresses"] = vout['scriptPubKey']['addresses']
                address["outputId"] = str(vout["n"]) + "_" + raw_tx["txid"]
                address["value"] = vout["value"]
                addresses.append(address.copy())

            else:
                break

    return addresses




