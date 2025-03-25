from enum import Enum


class USTMarketDataField(Enum):
    symbol = 55
    text = 58
    last_price = 31
    last_size = 7059
    last_yield = 7698
    last_exch = 7058
    avg_price = 74
    bid_price = 84
    bid_size = 88
    bid_yield = 7699
    bid_exch = 7068
    ask_price = 86
    ask_size = 85
    ask_yield = 7720
    ask_exch = 7057
    volume = 87
    avg_volume = 7282
    exchange = 6004
    con_id = 6008
    marker = 6119
    underlying_conid = 6457
    mkt_data_avail = 6509
    company = 7051
    contract_description = 7219
    listing_exchange = 7221
    shortable_shares = 7636
