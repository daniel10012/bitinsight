from src import CONFIG
import pandas as pd
import awswrangler as wr
import requests


# scrape OFAC list

def get_ofac_addresses():

    r = requests.get('https://www.treasury.gov/ofac/downloads/sdn.csv')
    string = r.text
    indicator = "Digital Currency Address - "
    items = []
    for line in string.splitlines():
        if indicator in line:
            item = {}
            addresses = []
            elements = line.split()
            item["abuser"] = elements[0].split(",")[1] + " " + elements[1].split(",")[0]
            for el in elements:
                if elements[elements.index(el) - 1] == "XBT":
                    addresses.append(el.rstrip(";"))
                    item["addresses"] = addresses
            items.append(item.copy())
    return(items)


# get parsed OFAC in pd frame

ofac_list = get_ofac_addresses()
ofac_pf = pd.DataFrame(ofac_list).explode("addresses")
ofac_pf["flagger"] = "OFAC"
ofac_pf["type"] = "OFAC list"
ofac_pf.columns = ['abuser', 'address', 'flagger', 'type']
ofac_pf = ofac_pf[['address', 'flagger', 'type', 'abuser']]


# parse Bitcoin abuse to pd frame

babuse_pf = pd.read_csv("flagged_data/records_forever.csv", encoding = "ISO-8859-1", sep=',', error_bad_lines=False, index_col=False, dtype=str)
babuse_pf = babuse_pf.drop_duplicates()
babuse_pf['flagger']='Bitcoinabuse'
babuse_pf = babuse_pf[["address", "flagger", "abuse_type_other", "abuser" ]]
babuse_pf['abuse_type_other'] = babuse_pf.abuse_type_other.str.replace('[\\,\"]', '')
babuse_pf['abuser'] = babuse_pf.abuser.str.replace('[\\,\"]', '')
babuse_pf.columns = ['address', 'flagger', 'type', 'abuser']


# concatenate OFAC and Bitcoin abuse

flagged_pf = babuse_pf.append(ofac_pf, ignore_index=True)
flagged_pf = flagged_pf.drop_duplicates(subset=['address'])


# Write consolidated frame to parquet

wr.s3.to_parquet(
    df=flagged_pf,
    path=f"{CONFIG.S3flagged}/flagged_pf.parquet"
)


