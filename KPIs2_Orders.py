"""
KPIs2_Orders
"""

import math
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import urllib3
from scipy import stats

import config
from leaky_bucket import leaky_bucket

# Disable SSL Warnings (for external API requests)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_acct_dets():
    url = f"{config.IBKR_BASE_URL}/v1/api/iserver/account/pnl/partitioned"

    # Wait until an API request token is available (rate-limiting control)
    leaky_bucket.wait_for_token()

    # Sending the GET request
    pnl_res = requests.get(url=url, verify=False)
    pnl_json = pnl_res.json()  # Parse JSON directly
    
    # Use the IBKR_ACCT_ID to get the value of 'nl'
    acct_key = f"{config.IBKR_ACCT_ID}.Core"
    return pnl_json.get("upnl", {}).get(acct_key, {}).get("nl")  # using nl in the interim


def calculate_quantities_with_sma(HEDGES_Combos):
    SMA = get_acct_dets()*4
    print(SMA)
    calculate_quantities(HEDGES_Combos, SMA)


def calculate_quantities(HEDGES_Combos, SMA):
    # Extract multipliers and dv01 values
    front_multiplier = HEDGES_Combos['A_FUT_Multiplier']
    back_multiplier = HEDGES_Combos['B_FUT_Multiplier']
    front_dv01 = HEDGES_Combos['A_FUT_DV01']
    back_dv01 = HEDGES_Combos['B_FUT_DV01']

    dv01_avg = (front_dv01 + back_dv01) / 2

    Q = {'A': 0, 'B': 0, 'G': 0}  # Initialize the dictionary with values

    # Use element-wise comparison and check if all conditions are met
    if (front_multiplier == 1000).all() and (back_multiplier == 1000).all():
        Q['A'] = back_dv01 / dv01_avg
        Q['B'] = front_dv01 / dv01_avg

    elif (front_multiplier == 2000).all() and (back_multiplier == 1000).all():
        Q['A'] = (1) * (back_dv01 / dv01_avg)
        Q['B'] = (1 / 2) * (front_dv01 / dv01_avg)

    elif (front_multiplier == 1000).all() and (back_multiplier == 2000).all():
        Q['A'] = (1 / 2) * (back_dv01 / dv01_avg)
        Q['B'] = (1) * (front_dv01 / dv01_avg)

    elif (front_multiplier == 2000).all() and (back_multiplier == 2000).all():
        Q['A'] = (1 / 2) * (back_dv01 / dv01_avg)
        Q['B'] = (1 / 2) * (front_dv01 / dv01_avg)

    # Ensure the sum of Q values does not exceed 0.9 * SMA
    total_Q = (Q['A'] * HEDGES_Combos['A_FUT_Multiplier'] * HEDGES_Combos['A_FUT_Price']).sum() + \
              (Q['B'] * HEDGES_Combos['B_FUT_Multiplier'] * HEDGES_Combos['B_FUT_Price']).sum()

    if total_Q >= 0.9 * SMA:
        scaling_factor = (0.9 * SMA) / total_Q
        Q = {k: v * scaling_factor for k, v in Q.items()}

    # Reduce Q values if they are divisible by a common denominator
    gcd_value = math.gcd(
        math.floor(Q['A'].iloc[0]),
        math.floor(Q['B'].iloc[0]) if len(Q) > 1 else abs(next(iter(Q.values())))
    )
    Q['G'] = gcd_value
    if gcd_value > 1:
        Q['A'] = Q['A'] // Q['G']
        Q['B'] = Q['B'] // Q['G']
        
    # Convert matDate columns to datetime objects
    HEDGES_Combos['A_CTD_matDate'] = pd.to_datetime(HEDGES_Combos['A_CTD_matDate'])
    HEDGES_Combos['B_CTD_matDate'] = pd.to_datetime(HEDGES_Combos['B_CTD_matDate'])

    # Get the current date for calculating days to maturity
    current_date = pd.to_datetime(datetime.now())

    # Function to calculate implied repo rate
    def implied_repo_rate(fut_price, ctd_price, mat_date):
        days_to_maturity = (mat_date - current_date).days
        if days_to_maturity > 0:
            return ((fut_price - ctd_price) / ctd_price) * (360 / days_to_maturity)
        return np.nan

    # Function to calculate gross basis
    def gross_basis(fut_price, ctd_price):
        return fut_price - ctd_price

    # Function to calculate carry
    def carry(fut_price, ctd_price, cf, ytm):
        return fut_price - (ctd_price * (cf / 100)) - (ytm * ctd_price)

    # Function to calculate convexity yield
    def convexity_yield(aprx_cvx, fut_price, ctd_price):
        return (aprx_cvx / 100) * (fut_price - ctd_price)

    # Apply the functions to the dataframe
    HEDGES_Combos['A_ImpliedRepoRate'] = HEDGES_Combos.apply(
        lambda row: implied_repo_rate(row['A_FUT_Price'], row['A_CTD_price'], row['A_CTD_matDate']), axis=1
    )
    HEDGES_Combos['B_ImpliedRepoRate'] = HEDGES_Combos.apply(
        lambda row: implied_repo_rate(row['B_FUT_Price'], row['B_CTD_price'], row['B_CTD_matDate']), axis=1
    )

    HEDGES_Combos['A_GrossBasis'] = HEDGES_Combos.apply(
        lambda row: gross_basis(row['A_FUT_Price'], row['A_CTD_price']), axis=1
    )
    HEDGES_Combos['B_GrossBasis'] = HEDGES_Combos.apply(
        lambda row: gross_basis(row['B_FUT_Price'], row['B_CTD_price']), axis=1
    )

    HEDGES_Combos['A_ConvexityYield'] = HEDGES_Combos.apply(
        lambda row: convexity_yield(row['A_FUT_AprxCvx'], row['A_FUT_Price'], row['A_CTD_price']), axis=1
    )
    HEDGES_Combos['B_ConvexityYield'] = HEDGES_Combos.apply(
        lambda row: convexity_yield(row['B_FUT_AprxCvx'], row['B_FUT_Price'], row['B_CTD_price']), axis=1
    )

    HEDGES_Combos['A_NetBasis'] = HEDGES_Combos['A_GrossBasis'] + HEDGES_Combos['A_ConvexityYield']
    HEDGES_Combos['B_NetBasis'] = HEDGES_Combos['B_GrossBasis'] + HEDGES_Combos['B_ConvexityYield']

    # Compute Adjusted Net Basis
    HEDGES_Combos['A_AdjNetBasis'] = HEDGES_Combos['A_NetBasis'] * Q['A']
    HEDGES_Combos['B_AdjNetBasis'] = HEDGES_Combos['B_NetBasis'] * Q['B']

    # Compute Pairs Adjusted Net Basis
    HEDGES_Combos['PairsAdjNetBasis'] = HEDGES_Combos['A_AdjNetBasis'] + HEDGES_Combos['B_AdjNetBasis'] #price is 1/2 of this sum in the order parameters

    # Calculate Z-scores of the log volumes directly from the HEDGES_Combos dataframe
    z_scores_A = stats.zscore(HEDGES_Combos['A_FUT_Volume'].apply(np.log))
    z_scores_B = stats.zscore(HEDGES_Combos['B_FUT_Volume'].apply(np.log))

    # Calculate average Z-scores and add it as a new column
    HEDGES_Combos['Ln_Avg_Vol'] = (z_scores_A + z_scores_B) / 2

    # Calculate RENTD
    HEDGES_Combos['RENTD'] = HEDGES_Combos['PairsAdjNetBasis'] * HEDGES_Combos['Ln_Avg_Vol']
    print(HEDGES_Combos['RENTD'])

    # Sort the dataframe by the RENTD column
    HEDGES_Combos = HEDGES_Combos.sort_values(by='RENTD', ascending=False)

    # Store the results of Q['A'], Q['B'], Q['G'] into new columns
    HEDGES_Combos['A_Q_Value'] = Q['A']
    HEDGES_Combos['B_Q_Value'] = Q['B']
    HEDGES_Combos['PairsGCD'] = Q['G']
    print(f"the front leg ratio is {HEDGES_Combos['A_Q_Value']} and the back leg is {HEDGES_Combos['B_Q_Value']}")
    
    # Apply the condition to individual values based on net_basis_diff
    HEDGES_Combos['A_Q_Value'] = HEDGES_Combos.apply(
        lambda row: row['A_Q_Value'] * -1 if (row['A_AdjNetBasis'] - row['B_AdjNetBasis']) > 0 else row['A_Q_Value'],
        axis=1)
    HEDGES_Combos['B_Q_Value'] = HEDGES_Combos.apply(
        lambda row: row['B_Q_Value'] * -1 if (row['A_AdjNetBasis'] - row['B_AdjNetBasis']) < 0 else row['B_Q_Value'],
        axis=1)

    # Extract order quantities and multipliers for top-ranked RENTD_Pairs prepped for riskmgmt
    # controls tests and alternative order instructions if failed
    top_row = HEDGES_Combos.iloc[0]
    if2risky = HEDGES_Combos.iloc[1]
    if2riskyagain = HEDGES_Combos.iloc[2]
    config.ORDERS = [
        {
            "front_conId": top_row['A_CTD_conId'],
            "front_ratio": top_row['A_Q_Value'],
            "back_conId": top_row['B_CTD_conId'],
            "back_ratio": top_row['B_Q_Value'],
            "quantity": top_row['PairsGCD'],
            "price": -0.5 * top_row['PairsAdjNetBasis']
        },
        {
            "front_conId": if2risky['A_CTD_conId'],
            "front_ratio": if2risky['A_Q_Value'],
            "back_conId": if2risky['B_CTD_conId'],
            "back_ratio": if2risky['B_Q_Value'],
            "quantity": if2risky['PairsGCD'],
            "price": -0.5 * if2risky['PairsAdjNetBasis']
        },
        {
            "front_conId": if2riskyagain['A_CTD_conId'],
            "front_ratio": if2riskyagain['A_Q_Value'],
            "back_conId": if2riskyagain['B_CTD_conId'],
            "back_ratio": if2riskyagain['B_Q_Value'],
            "quantity": if2riskyagain['PairsGCD'],
            "price": -0.5 * if2riskyagain['PairsAdjNetBasis']
        }
    ]
    
    config.HEDGES_Combos = HEDGES_Combos
    print(HEDGES_Combos)

    return config.ORDERS
