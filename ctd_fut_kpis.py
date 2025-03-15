"""
CTD and FUT KPIs
"""

import itertools

import pandas as pd

import config
from fixed_income_calc import (BPrice, MDur, MacDur, DV01,
                               approximate_duration,
                               approximate_convexity)  # Import all required functions and variables


# Example usage of HEDGES dataframe and the fixed_income_calc functions
def display_hedges_info():
    print("Displaying first 5 rows of HEDGES dataframe:")
    print(config.HEDGES.head())


def run_fixed_income_calculation(HEDGES):
    # Define parameters with assignment statements
    period = 2  # Constant value
    day_count = 1  # Constant value

    # Compute values for each row and assign them to new columns
    HEDGES['CTD_BPrice'] = HEDGES.apply(lambda row: BPrice(cpn=row['CTD_coupon_rate'],
                                                           term=row['CTD_ytm'],
                                                           yield_=row['CTD_yield'],
                                                           period=period,
                                                           begin=row['CTD_prev_cpn'],
                                                           next_coupon=row['CTD_ncpdt'],
                                                           day_count=day_count), axis=1)

    HEDGES['CTD_MDur'] = HEDGES.apply(lambda row: MDur(cpn=row['CTD_coupon_rate'],
                                                       term=row['CTD_ytm'],
                                                       yield_=row['CTD_yield'],
                                                       period=period,
                                                       begin=row['CTD_prev_cpn'],
                                                       next_coupon=row['CTD_ncpdt'],
                                                       day_count=day_count), axis=1)

    HEDGES['CTD_MacDur'] = HEDGES.apply(lambda row: MacDur(cpn=row['CTD_coupon_rate'],
                                                           term=row['CTD_ytm'],
                                                           yield_=row['CTD_yield'],
                                                           period=period,
                                                           begin=row['CTD_prev_cpn'],
                                                           next_coupon=row['CTD_ncpdt'],
                                                           day_count=day_count), axis=1)

    HEDGES['CTD_DV01'] = HEDGES.apply(lambda row: DV01(cpn=row['CTD_coupon_rate'],
                                                       term=row['CTD_ytm'],
                                                       yield_=row['CTD_yield'],
                                                       period=period,
                                                       begin=row['CTD_prev_cpn'],
                                                       next_coupon=row['CTD_ncpdt'],
                                                       day_count=day_count), axis=1)

    HEDGES['CTD_AprxDur'] = HEDGES.apply(lambda row: approximate_duration(cpn=row['CTD_coupon_rate'],
                                                                          term=row['CTD_ytm'],
                                                                          yield_=row['CTD_yield'],
                                                                          period=period,
                                                                          begin=row['CTD_prev_cpn'],
                                                                          next_coupon=row['CTD_ncpdt'],
                                                                          day_count=day_count), axis=1)

    HEDGES['CTD_AprxCvx'] = HEDGES.apply(lambda row: approximate_convexity(cpn=row['CTD_coupon_rate'],
                                                                           term=row['CTD_ytm'],
                                                                           yield_=row['CTD_yield'],
                                                                           period=period,
                                                                           begin=row['CTD_prev_cpn'],
                                                                           next_coupon=row['CTD_ncpdt'],
                                                                           day_count=day_count), axis=1)

    # Now, perform the division and store the results in new columns
    HEDGES['FUT_Price'] = HEDGES['CTD_BPrice'] / HEDGES['CTD_CF']
    HEDGES['FUT_MDur'] = HEDGES['CTD_MDur'] / HEDGES['CTD_CF']
    HEDGES['FUT_MacDur'] = HEDGES['CTD_MacDur'] / HEDGES['CTD_CF']
    HEDGES['FUT_DV01'] = HEDGES['CTD_DV01'] / HEDGES['CTD_CF']
    HEDGES['FUT_AprxDur'] = HEDGES['CTD_AprxDur'] / HEDGES['CTD_CF']
    HEDGES['FUT_AprxCvx'] = HEDGES['CTD_AprxCvx'] / HEDGES['CTD_CF']

    # Generate all combinations of rows from HEDGES (pairing them side by side)
    combinations = [(row1, row2) for row1, row2 in itertools.product(HEDGES.iterrows(), repeat=2)
                    if row1[1]['CTD_conId'] != row2[1]['CTD_conId']] # do not pair same contracts

    # Prepare a list to store the combinations in the new dataframe format
    combos_data = []

    for combo in combinations:
        row1, row2 = combo  # Get the two rows from the combination

        # Prefix the headers of row1 with 'A_' and row2 with 'B_'
        row1_data = {f'A_{key}': value for key, value in row1[1].to_dict().items()}
        row2_data = {f'B_{key}': value for key, value in row2[1].to_dict().items()}

        # Combine the two rows
        combined_row = {**row1_data, **row2_data}
        combos_data.append(combined_row)

    # Create a new DataFrame for HEDGES_Combos object (in the config.py) and populate it
    config.HEDGES_Combos = pd.DataFrame(combos_data)


if __name__ == "__main__":
    display_hedges_info()
    run_fixed_income_calculation(config.HEDGES)
