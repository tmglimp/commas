from datetime import datetime, timedelta
from math import pow
import pandas as pd

# Functions for fixed income calculations.
# These functions cover bond price (BPrice), accrued interest (AInt), modified duration (MDur), Macaulay duration (MacDur), DV01, convexity measure (Cvx), and yield-to-price (P2Y).
# Inputs:
#   cpn     - annual coupon, ex: 4.5
#   term    - term to maturity in years
#   yield   - annual pricing yield in decimal form, ex: 0.035
#   period  - number of payments per year, default is 2
#   begin   - date of begin accrual, ex: 20180815 (when coupon was last paid to bondholder) - coupon_prev_date
#   settle  - settlement date, ex: 20181005 (when bond was completely sold to buyer) - today
#   next_coupon    - next coupon date - coupon_ncpdt
#   day_count   - 1 (default) actual/actual, anything else for 30/360
#   Default is for Last, Settle and Next set to NULL which assumes that the settlement is on a coupon date.
#   Exact pricing is implemented for BPrice (bond price), MDur (modified duration), MacDur (Macaulay duration),
#   DV01 (change is value for a 1 bp yield change)
#   Cvx (convexity measure) implemented for use on coupon dates and for intra-settlement use

# Helper function to calculate term
def calculate_term(settlement_date_str, maturity_date_str, day_count_convention=365.25):
    """
    Calculate the time to maturity (term) in years based on a day count convention.

    Args:
        settlement_date_str (str): Settlement date in 'YYYYMMDD' format.
        maturity_date_str (str): Maturity date in 'YYYYMMDD' format.
        day_count_convention (float, optional): Day count convention to use for year calculation.
                                                Defaults to 365.25 for Actual/Actual (ISDA).

    Returns:
        float: Time to maturity in years.
    """
    # Parse the settlement and maturity dates
    settlement_date = datetime.strptime(settlement_date_str, '%Y%m%d')
    maturity_date = datetime.strptime(maturity_date_str, '%Y%m%d')

    # Calculate the difference in days
    days_to_maturity = (maturity_date - settlement_date).days

    # Convert to years using the specified day count convention
    term_in_years = days_to_maturity / day_count_convention
    return term_in_years

def compute_settlement_date(trade_date, t_plus=1):
    """
    Calculate the settlement date considering weekends.

    Args:
        trade_date (str or datetime): Trade date in 'YYYYMMDD' format or a datetime object.
        t_plus (int): Settlement delay in business days (e.g., 2 for T+2).

    Returns:
        datetime: Settlement date adjusted for weekends.
    """
    # Convert trade_date to datetime if provided as a string
    if isinstance(trade_date, str):
        trade_date = datetime.strptime(trade_date, '%Y%m%d')

    # Start adding days from the trade date
    settlement_date = trade_date
    business_days_added = 0

    while business_days_added < t_plus:
        settlement_date += timedelta(days=1)  # Move to the next calendar day
        # Check if the day is a business day (not Saturday or Sunday)
        if settlement_date.weekday() < 5:  # 0 = Monday, 6 = Sunday
            business_days_added += 1

    return settlement_date.strftime('%Y%m%d')

def calculate_ytm(market_price, face_value, coupon_rate, time_to_maturity, periods_per_year=2, n_digits=2):
    """
        Calculate Yield to Maturity (YTM) of a bond.

        Args:
            market_price (float): Current market price of the bond.
            face_value (float): Face value (par value) of the bond.
            coupon_rate (float): Annual coupon rate (as a decimal, e.g., 0.05 for 5%).
            time_to_maturity (float): Number of years until maturity.
            periods_per_year (int): Number of coupon payments per year (default is 1).
            n_digits (int): Number of decimal places to return (default is 2).

        Returns:
            float: Approximate Yield to Maturity (YTM) as a decimal.
        """
    market_price = market_price / 100.0 * face_value
    coupon_rate = coupon_rate / 100.0
    coupon_payment = face_value * coupon_rate / periods_per_year

    def bond_price(ytm):
        pv = 0
        for t in range(1, int(time_to_maturity * periods_per_year) + 1):
            pv += coupon_payment / (1 + ytm / periods_per_year) ** t
        pv += face_value / (1 + ytm / periods_per_year) ** int(time_to_maturity * periods_per_year)
        return pv

    ytm_guess = coupon_rate
    tolerance = 1e-8
    max_iterations = 1000
    ytm = ytm_guess

    for _ in range(max_iterations):

        price_at_ytm = bond_price(ytm)

        # The derivative of the bond price with respect to YTM
        delta_ytm = 1e-5
        price_up = bond_price(ytm + delta_ytm)
        price_down = bond_price(ytm - delta_ytm)
        price_derivative = (price_up - price_down) / (2 * delta_ytm)

        if abs(price_derivative) < 1e-12:
            print("Derivative too small, may not have converged correctly.")
            return ytm

        # Calculate the new YTM using the Newton-Raphson method
        ytm_new = ytm - (price_at_ytm - market_price) / price_derivative

        if abs(ytm_new - ytm) < tolerance:
            return round(ytm_new, n_digits)
        ytm = ytm_new
    print("YTM calculation did not converge within the maximum number of iterations.")
    return ytm

# Helper function to calculate accrual period
def accrual_period(begin, settle, next_coupon, day_count=1):
    """
    Computes the accrual period for both day count conventions (actual/actual or 30/360). This function calculates
    the time (in years) between two dates, which is essential for determining accrued interest or payments for bonds.
    """
    if day_count == 1:  # Actual/Actual
        L = datetime.strptime(str(begin), '%Y%m%d')
        S = datetime.strptime(str(settle), '%Y%m%d')
        N = datetime.strptime(str(next_coupon if next_coupon is not None else settle), '%Y%m%d')
        return (S - L).days / (N - L).days
    else:  # 30/360
        L = [int(begin[:4]), int(begin[4:6]), int(begin[6:8])]
        S = [int(settle[:4]), int(settle[4:6]), int(settle[6:8])]
        return (360 * (S[0] - L[0]) + 30 * (S[1] - L[1]) + S[2] - L[2]) / 180

# Accrued interest
def AInt(cpn, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    This computes the interest that has accumulated since the last coupon payment but hasn't been paid yet.
    """
    v = accrual_period(begin, settle, next_coupon, day_count)
    return cpn / period * v

# Bond price
def BPrice(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    Calculates the bond price without accrued interest, including intra-coupon settlement adjustments.
    The formula accounts for: (1) Present value of future coupon payments and the face value of the bond;
    (2) Adjustments for settlement date (if it's between coupon dates).
    The price adjusts based on the time until the next payment (intra-coupon settlement).
    """
    T = term * period  # Total coupon periods (total number of times coupon is paid)
    C = cpn / period  # Periodic coupon
    Y = yield_ / period  # Periodic yield

    # Price without accrual (clean price) - fi-set-4-slide-9
    price = C * (1 - pow(1 + Y, -T)) / Y + 100 / pow(1 + Y, T)

    # Price with accrual (dirty price)
    if begin and settle and next_coupon:
        v = accrual_period(begin, settle, next_coupon, day_count)
        price = pow(1 + Y, v) * price - v * C
    return price

# Modified duration
def MDur(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    Computes the modified duration (derived from Macaulay Duration). It measures how sensitive the bond price is to changes in yield.
    """
    T = term * period  # Total coupon periods (total number of times coupon is paid)
    C = cpn / period  # Periodic coupon
    Y = yield_ / period  # Periodic yield
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)  # Bond price

    if begin and settle and next_coupon:
        v = accrual_period(begin, settle, next_coupon, day_count)
        P = pow(1 + Y, v) * P
        mdur = (
                -v * pow(1 + Y, v - 1) * C / Y * (1 - pow(1 + Y, -T))
                + pow(1 + Y, v) * (
                        C / pow(Y, 2) * (1 - pow(1 + Y, -T))
                        - T * C / (Y * pow(1 + Y, T + 1))
                        + (T - v) * 100 / pow(1 + Y, T + 1)
                )
        )
    else:
        # fi-set-4-slide-9
        mdur = (C / pow(Y, 2) * (1 - pow(1 + Y, -T))) + (T * (100 - C / Y) / pow(1 + Y, T + 1))
    return mdur / (period * P)

# Macaulay duration
def MacDur(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    Computes the Macaulay duration using the modified duration. It calculates the weighted average time to receive cash
    flows from the bond, adjusted by present value. It's expressed in years.
    """
    return MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count) * (1 + yield_ / period)

# DV01
def DV01(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    Calculates DV01 as the product of price and modified duration. It measures the dollar price change for a 1 basis
    point (0.01%) change in yield.
    """
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    return round(MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count) * P, 5)

# Convexity Measure
def Cvx(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    Computes the convexity measure. It measures the curvature of the price-yield relationship, improving accuracy
    beyond Modified Duration. Higher convexity means greater sensitivity to interst change.
    """
    T = term * period
    C = cpn / period
    Y = yield_ / period
    P = BPrice(cpn, term, yield_, period)

    if begin and settle and next_coupon:
        # fi-set-4-slide-26
        v = accrual_period(begin, settle, next_coupon, day_count)
        P = pow(1 + Y, v) * P
        dcv = (-v * (v - 1) * pow(1 + Y, v - 2) * C / Y * (1 - pow(1 + Y, -T)) -
               2 * v * pow(1 + Y, v - 1) * (C / pow(Y, 2) * (1 - pow(1 + Y, -T)) -
                                            T * C / (Y * pow(1 + Y, T + 1))) -
               pow(1 + Y, v) * (-C / pow(Y, 3) * (1 - pow(1 + Y, -T)) +
                                2 * T * C / (pow(Y, 2) * pow(1 + Y, T + 1)) +
                                T * (T + 1) * C / (Y * pow(1 + Y, T + 2))) +
               (T - v) * (T + 1) * 100 / pow(1 + Y, T + 2 - v)
               )
    else:
        # fi-set-4-slide-16
        dcv = (2 * C / pow(Y, 3) * (1 - pow(1 + Y, -T)) -
               2 * T * C / (pow(Y, 2) * pow(1 + Y, T + 1)) +
               T * (T + 1) * (100 - C / Y) / pow(1 + Y, T + 2))
    return dcv / (P * period ** 2)

# Price to yield
def P2Y(price, cpn, term=10, period=2, begin=None, settle=None, next_coupon=None):
    """
    Uses scipy numerical optimization to solve for yield-to-maturity given a bond price. This finds the Yield to
    Maturity (YTM), solving numerically for the discount rate that equates the bond's price to the present value of
    its cash flows.
    """
    from scipy.optimize import minimize_scalar

    def objective(yield_):
        return (price - BPrice(cpn, term, yield_, period, begin, settle, next_coupon)) ** 2
    result = minimize_scalar(objective, bounds=(-0.5, 1), method='bounded')
    return result.x

def approximate_duration(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,
                         delta_y=0.0001):
    """
    Calculates the approximate duration of a bond using the finite difference method.
    Args:
        cpn (float): Annual coupon rate (as a percentage, e.g., 2.5 for 2.5%).
        term (float): Time to maturity in years.
        yield_ (float): Current yield to maturity (as a decimal, e.g., 0.03 for 3%).
        period (int): Number of coupon payments per year (default: 2 for semiannual).
        begin, settle, next_coupon (str): Dates used for intra-coupon adjustments.
        day_count (int): Day count convention (1 = Actual/Actual, etc.).
        delta_y (float): Small change in yield used for finite difference calculation (default: 0.0001 or 1 basis points).

    Returns:
        float: Approximate duration.
    """
    # Calculate bond prices at Y, Y+delta_y, and Y-delta_y
    price = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    price_up = BPrice(cpn, term, (yield_ + delta_y), period, begin, settle, next_coupon, day_count)
    price_down = BPrice(cpn, term, (yield_ - delta_y), period, begin, settle, next_coupon, day_count)

    # Approximate duration formula: fi-set-4-slide-21
    duration = (price_down - price_up) / (2 * price * delta_y)
    return duration

def approximate_convexity(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,
                          delta_y=0.0001):
    """
    Calculates the approximate convexity of a bond using the finite difference method.
    Args:
        cpn (float): Annual coupon rate (as a percentage, e.g., 2.5 for 2.5%).
        term (float): Time to maturity in years.
        yield_ (float): Current yield to maturity (as a decimal, e.g., 0.03 for 3%).
        period (int): Number of coupon payments per year (default: 2 for semiannual).
        begin, settle, next_coupon (str): Dates used for intra-coupon adjustments.
        day_count (int): Day count convention (1 = Actual/Actual, etc.).
        delta_y (float): Small change in yield used for finite difference calculation (default: 0.0001 or 1 basis points).

    Returns:
        float: Approximate convexity.
    """
    # Calculate bond prices at Y, Y+delta_y, and Y-delta_y
    price = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    price_up = BPrice(cpn, term, (yield_ + delta_y), period, begin, settle, next_coupon, day_count)
    price_down = BPrice(cpn, term, (yield_ - delta_y), period, begin, settle, next_coupon, day_count)

    # Approximate convexity formula: fi-set-4-slide-21
    convexity = (price_down + price_up - 2 * price) / (price * delta_y ** 2)
    return convexity

def calculate_bond_metrics(face_value, market_price, issue_date_str, maturity_date_str, coupon_rate,
                           periods_per_year, day_count, coupon_prev_date_str=None, coupon_next_date_str=None,
                           trade_settle_date_str=None, market_yield=None):
    # print('Input:')
    # print(
    #     f'face_value => {face_value}\nmarket_price => {market_price}\nissue_date_str => {issue_date_str}\nmaturity_date_str => {maturity_date_str}\n'
    #     f'coupon_rate => {coupon_rate}\nperiods_per_year => {periods_per_year}\nday_count => {day_count}\ncoupon_prev_date_str => {coupon_prev_date_str}\n'
    #     f'coupon_next_date_str => {coupon_next_date_str}\nsettle_date_str => {trade_settle_date_str}')

    begin = coupon_prev_date_str if coupon_prev_date_str is not None else issue_date_str
    settle = trade_settle_date_str if trade_settle_date_str is not None else maturity_date_str
    effective_date = trade_settle_date_str if trade_settle_date_str is not None else begin

    time_to_maturity = calculate_term(effective_date, maturity_date_str)

    # yield_to_maturity = P2Y(market_price, coupon_rate, int(time_to_maturity), periods_per_year, begin, settle,
    #                        coupon_next_date_str)
    ytm = calculate_ytm(market_price, face_value, coupon_rate, time_to_maturity, periods_per_year, 5)
    yield_to_maturity = market_yield if market_yield is not None else ytm
    bond_price = BPrice(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                        settle, coupon_next_date_str, day_count)
    accrued_interest = AInt(coupon_rate, periods_per_year, begin, settle, coupon_next_date_str, day_count)
    modified_duration = MDur(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                             settle, coupon_next_date_str, day_count)
    macaulay_duration = MacDur(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                               settle, coupon_next_date_str, day_count)
    dv01 = DV01(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                settle, coupon_next_date_str, day_count)
    convexity = Cvx(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                    settle, coupon_next_date_str, day_count)
    approx_duration = approximate_duration(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                                           settle, coupon_next_date_str, day_count)
    approx_convexity = approximate_convexity(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                                             settle, coupon_next_date_str, day_count)
    return {
        "time_to_maturity": time_to_maturity,
        "accrued_interest": accrued_interest,
        "yield_to_maturity": ytm,
        "clean_price": bond_price,
        "dirty_price": bond_price,
        "macaulay_duration": macaulay_duration,
        "modified_duration": modified_duration,
        "convexity": convexity,
        "dv01": dv01,
        "approx_duration": approx_duration,
        "approx_convexity": approx_convexity,
    }

def compute_ust_kpis(item):
    """
    Computes key performance indicators (KPIs) for a U.S. Treasury bond based on provided bond attributes.
    - Validates required fields such as issue date, maturity date, coupon rate, and market prices.
    - Converts numeric fields to appropriate formats for calculations.
    - Determines settlement date using a T+1 convention.
    - Computes bond price, yield to maturity, accrued interest, duration (modified and Macaulay),
      convexity, DV01, and effective duration/convexity.
    - Uses midpoint pricing from ask, bid, and last price for a fair market estimate.
    - Supports calculation adjustments for coupon payments within the bond's lifecycle.
    - Returns a dictionary containing computed bond metrics if validation passes, otherwise returns None.
    """

    issue_date = item['issue_date']
    maturity_date = item['maturity_date']
    coupon_rate = item['coupon_rate']
    coupon_prev_date = item['coupon_prev_date']
    coupon_next_date = item['coupon_ncpdt']
    face_value = item['principal_value']
    ask_price = item['ask_price']
    bid_price = item['bid_price']
    last_price = item['last_price']
    market_yield = None
    trade_date = compute_settlement_date(datetime.today().strftime('%Y%m%d'))  # using T+1 as settlement date

    is_valid = (
            pd.notna(ask_price) and ask_price != "" and
            pd.notna(bid_price) and bid_price != "" and
            pd.notna(issue_date) and issue_date != "" and
            pd.notna(maturity_date) and maturity_date != "" and
            pd.notna(coupon_rate) and coupon_rate != "" and
            pd.notna(coupon_prev_date) and coupon_prev_date != "" and
            pd.notna(coupon_next_date) and coupon_next_date != "" and
            pd.notna(face_value) and face_value != ""
    )

    if is_valid:
        coupon_rate = float(coupon_rate)
        issue_date = str(int(issue_date))
        maturity_date = str(int(maturity_date))
        coupon_prev_date = str(int(coupon_prev_date))
        coupon_next_date = str(int(coupon_next_date))
        periods_per_year = 2  # coupon payment for bond made twice a year
        day_count = 1  # use actual/actual convention to compute accrual period
        current_market_price = (float(ask_price) + float(bid_price) + float(
            last_price)) / 3  # midpoint price between ask and bid for now
        should_calculate_in_between_coupons = True

        if not should_calculate_in_between_coupons:
            coupon_next_date = None
            trade_date = None

        bond_metrics = calculate_bond_metrics(face_value, current_market_price, issue_date, maturity_date,
                                              coupon_rate,
                                              periods_per_year,
                                              day_count,
                                              coupon_prev_date,
                                              coupon_next_date,
                                              trade_date,
                                              market_yield
                                              )

        return {
            "time_to_maturity": bond_metrics.get('time_to_maturity') if is_valid and bond_metrics is not None else None,
            "mols_bond_price": bond_metrics.get('clean_price') if is_valid and bond_metrics is not None else None,
            "mols_yield_to_maturity": bond_metrics.get(
                'yield_to_maturity') if is_valid and bond_metrics is not None else None,
            "mols_accrued_interest": bond_metrics.get(
                'accrued_interest') if is_valid and bond_metrics is not None else None,
            "mols_modified_duration": bond_metrics.get(
                'modified_duration') if is_valid and bond_metrics is not None else None,
            "mols_macaulay_duration": bond_metrics.get(
                'macaulay_duration') if is_valid and bond_metrics is not None else None,
            "mols_dv_01": bond_metrics.get('dv01') if is_valid and bond_metrics is not None else None,
            "mols_convexity_measure": bond_metrics.get('convexity') if is_valid and bond_metrics is not None else None,
            "effective_duration": bond_metrics.get(
                'approx_duration') if is_valid and bond_metrics is not None else None,
            "effective_convexity": bond_metrics.get(
                'approx_convexity') if is_valid and bond_metrics is not None else None,
        }
    else:
        return None
