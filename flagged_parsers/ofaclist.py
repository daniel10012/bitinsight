import requests
import datetime


'''
What is the structure of a digital currency address on OFAC’s SDN List?
Digital currency addresses listed on the SDN List include their unique
alphanumeric identifier (up to 256 characters) and identify the digital
currency to which the address corresponds (e.g., Bitcoin (XBT), Ethereum
(ETH), Litecoin (LTC), Neo (NEO), Dash (DASH), Ripple (XRP), Iota (MIOTA),
Monero (XMR), and Petro (PTR)). Each digital currency address listed on the SDN
list will have its own field: the structure will always begin with “Digital
Currency Address”, followed by a dash and the digital currency’s symbol 
(e.g., “Digital Currency Address - XBT”, “Digital Currency Address - ETH”).
This information is followed by the unique alphanumeric identifier of the
specific address. [06-06-2018]
'''


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




