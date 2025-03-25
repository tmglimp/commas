from enum import Enum

class FutMarketDataField(Enum):
    last_price = 31
    symbol = 55
    daily_pnl = 78
    realized_pnl = 79
    bid_price = 84
    ask_size = 85
    ask_price = 86
    volume = 87
    bid_size = 88
    right = 201
    exchange = 6004
    con_id = 6008
    months = 6072
    regular_expiry = 6073
    underlying_conid = 6457
    market_data_availability = 6509
    ask_exch = 7057
    last_exch = 7058
    last_size = 7059
    bid_exch = 7068
    implied_vol = 7084
    option_volume = 7089
    conid_exchange = 7094
    can_be_traded = 7184
    contract_desc = 7219
    contract_desc_2 = 7220
    listing_exch = 7221
    avg_volume = 7282
    shortable_shares = 7636
    fee_rate = 7637
    shortable = 7644
    futures_open_interest = 7697
