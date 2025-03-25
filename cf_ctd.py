"""
CF Lookup and CTD Search
"""

import pandas as pd

import config


def find_conversion_factor(FUTURES, USTs, file_path, index):
    # Define the mapping of futures symbols to sheet names and yield constraints
    sheet_mapping = {
        'ZT': ('2-Year Note Table', 1.75, 2),
        'Z3N': ('3-Year Note Table', 2.75, 3),
        'ZF': ('5-Year Note Table', 4.16667, 5.25),
        'ZN': ('10-Year Note Table', 6.5, 10),
        'TN': ('10-Year Note Table', 9.5, 10)
    }

    futures_symbol = FUTURES.loc[index, 'symbol']
    futures_price = FUTURES.loc[index, 'price']

    if futures_symbol not in sheet_mapping:
        raise ValueError("Invalid futures symbol")

    sheet_name, ytm_min, ytm_max = sheet_mapping[futures_symbol]
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    # Filter USTs by yield constraints
    filtered_USTs = USTs[(USTs['year_to_maturity'] > ytm_min) & (USTs['year_to_maturity'] < ytm_max)].copy()

    if filtered_USTs.empty:
        return None, None, None, None

    # Select the USTs['price'] value that minimizes the distance between CF = USTs['price'] * FUTURES['price']
    filtered_USTs['cf_target'] = filtered_USTs['price'] * futures_price
    selected_ust = filtered_USTs.iloc[(filtered_USTs['cf_target'] - filtered_USTs['price']).abs().argsort()[:1]]
    selected_ust_price = selected_ust['price'].values[0]
    selected_ust_row = selected_ust.iloc[0]

    # Determine the end of the table dynamically
    last_row = df.shape[0]
    last_col = df.shape[1]

    # Extract relevant sections
    coupon_column = df.iloc[5:last_row, 0]  # A6 onward for 'Coupon'
    cf_table = df.iloc[5:last_row, 1:last_col]  # Conversion factors
    years_months = df.iloc[4, 1:last_col].values  # B5 onward for 'Years-Months'

    # Find best CF match
    cf_values = cf_table.values
    best_match = None
    min_diff = float('inf')
    best_coupon = None
    best_years_months = None

    for i in range(cf_values.shape[0]):
        for j in range(cf_values.shape[1]):
            cf = cf_values[i, j]
            if pd.notna(cf):  # Ensure value is valid
                diff = abs(cf - selected_ust_price * futures_price)
                if diff < min_diff:
                    min_diff = diff
                    best_match = cf
                    best_coupon = coupon_column.iloc[i]
                    best_years_months = years_months[j]

    # Adjust coupon to match nearest quarter
    if best_coupon is not None:
        best_coupon = round_to_nearest_quarter(best_coupon)

    return best_match, best_coupon, best_years_months, selected_ust_row


def round_to_nearest_quarter(value):
    return round(value * 4) / 4


def parse_volume(volume_str):
    """Parse volume with suffixes like 'K', 'M', etc., into a float. IBKR doesn't return numeric volume values."""
    if not volume_str:  # Check if volume_str is empty or None
        return 0

    if isinstance(volume_str, str):
        if 'K' in volume_str.upper():  # Check if the string has 'K' (thousands)
            return float(volume_str.replace('K', '').replace('k', '')) * 1000
        elif 'M' in volume_str.upper():  # Check for 'M' (millions)
            return float(volume_str.replace('M', '').replace('m', '')) * 1000000
        elif 'B' in volume_str.upper():  # Check for 'B' (billions)
            return float(volume_str.replace('B', '').replace('b', '')) * 1000000000
        else:
            return float(volume_str)  # If no suffix, just return the float value
    return float(volume_str)  # If it's already a number, return it as float


# Process entire FUTURES dataset
def process_futures_data(FUTURES, USTs, file_path):
    cf_results = []
    ctd_data = {
        'CTD_CUSIP': [], 'CTD_conIdex': [], 'CTD_conId': [], 'CTD_price': [], 'CTD_yield': [],
        'CTD_coupon_rate': [], 'CTD_prev_cpn': [], 'CTD_ncpdt': [], 'CTD_matDate': [], 'CTD_CF': [], 'CTD_ytm': []
    }
    for index in FUTURES.index:
        cf, coupon, years_months, selected_ust = find_conversion_factor(FUTURES, USTs, file_path, index)
        cf_results.append(cf)

        if selected_ust is not None:
            ctd_data['CTD_CUSIP'].append(selected_ust['cusip'])
            ctd_data['CTD_conIdex'].append(selected_ust['con_id'])
            ctd_data['CTD_conId'].append(selected_ust['con_id'])
            ctd_data['CTD_price'].append(selected_ust['price'])
            ctd_data['CTD_yield'].append(selected_ust['yield'])
            ctd_data['CTD_coupon_rate'].append(selected_ust['coupon_rate'])
            ctd_data['CTD_prev_cpn'].append(selected_ust['coupon_prev_date'])
            ctd_data['CTD_ncpdt'].append(selected_ust['coupon_ncpdt'])
            ctd_data['CTD_matDate'].append(selected_ust['maturity_date'])
            ctd_data['CTD_ytm'].append(selected_ust['year_to_maturity'])
            ctd_data['CTD_CF'].append(cf)
        else:
            for key in ctd_data.keys():
                ctd_data[key].append(None)

    # Update FUTURES dataframe in config file
    config.FUTURES['CF'] = cf_results

    # Update HEDGES dataframe in config file by copying FUTURES and adding CTD columns
    for key, values in ctd_data.items():
        config.HEDGES[key] = values

    # Do we need future contracts we do not find CTD for? I'm suggesting we drop such category at this point
    config.HEDGES = config.HEDGES.dropna(subset=['CTD_CUSIP'])

    # Hold on to the multiplier, volume values. Needed in quantities calculation in KPIs2_Orders.py
    config.HEDGES['FUT_Multiplier'] = FUTURES['multiplier']
    config.HEDGES['FUT_Volume'] = FUTURES['volume'].apply(parse_volume)

    return config.HEDGES
