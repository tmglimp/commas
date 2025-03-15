import numpy as np
from datetime import datetime


def calculate_conversion_factor(coupon_rate, yrstomat, yield_rate=0.06):
    """
    Computes the conversion factor for a Treasury bond.
    :param coupon_rate: The bond's annual coupon rate (as a decimal).
    :param yrstomat: The number of years to maturity.
    :param yield_rate: The assumed yield rate for conversion factor calculation (default is 6%).
    :return: The conversion factor.
    """
    cf = (coupon_rate / yield_rate) * (1 - (1 + yield_rate) ** -yrstomat) + (1 + yield_rate) ** -yrstomat
    return cf 
    print(cf)

def calculate_ctd(bond_prices, conversion_factors, accrued_interest, futures_price):
    """
    Computes the Cheapest-to-Deliver (CTD) bond for a Treasury futures contract.
    :param bond_prices: Array of quoted bond prices.
    :param conversion_factors: Array of conversion factors for the bonds.
    :param accrued_interest: Array of accrued interest for the bonds.
    :param futures_price: The current futures price.
    :return: Index of the CTD bond and its net basis.
    """
    adjusted_futures_prices = futures_price * conversion_factors
    net_basis = bond_prices + accrued_interest - adjusted_futures_prices
    ctd_index = np.argmin(net_basis)
    return ctd_index, net_basis[ctd_index]


def calculate_implied_repo_rate(bond_price, conversion_factor, accrued_interest, futures_price, days_to_delivery):
    """
    Computes the implied repo rate for the CTD bond.
    :param bond_price: Price of the bond.
    :param conversion_factor: Conversion factor of the bond.
    :param accrued_interest: Accrued interest on the bond.
    :param futures_price: Futures contract price.
    :param days_to_delivery: Days until futures contract delivery.
    :return: Implied repo rate.
    """
    implied_repo = ((bond_price + accrued_interest) / (futures_price * conversion_factor) - 1) * (
                365 / days_to_delivery)
    return implied_repo


def calculate_convexity_yield(coupon_rate, yrstomat, market_yield):
    """
    Computes the convexity-adjusted yield for a bond.
    :param coupon_rate: The bond's annual coupon rate.
    :param yrstomat: The number of years to maturity.
    :param market_yield: Market yield for the bond.
    :return: Convexity-adjusted yield.
    """
    convexity_adjustment = (coupon_rate * (yrstomat ** 2 + yrstomat)) / ((1 + market_yield) ** 2)
    return market_yield + convexity_adjustment


if __name__ == "__main__":
    # Example data
    bond_coupons = np.array([.0225, 0.01875, .02625, 0.05])  # Annual coupon rates (underlyings) #0.9303, 0.9324, 0.8540, 0.9312
    bond_maturities = np.array([2, 1.75, 5.083333333, 9])  # Maturity in years (underlyings)
    # Compute conversion factors for each bond
    conversion_factors = np.array([calculate_conversion_factor(c, m) for c, m in zip(bond_coupons, bond_maturities)])
    print(conversion_factors)
    
#    market_yield = 0.05  # Example market yield
    # Compute CTD bond
 #   ctd_index, ctd_net_basis = calculate_ctd(bond_prices, conversion_factors, accrued_interest, futures_price)
    # Compute implied repo rate
  #  implied_repo_rate = calculate_implied_repo_rate(bond_prices[ctd_index], conversion_factors[ctd_index],
                                                #    accrued_interest[ctd_index], futures_price, days_to_delivery)
    # Compute convexity yield
   # convexity_yield = calculate_convexity_yield(bond_coupons[ctd_index], bond_maturities[ctd_index], market_yield)
    print(f"Conversion Factors: {conversion_factors}")
  # print(f"Cheapest-to-Deliver Bond Index: {ctd_index}")
  # print(f"CTD Net Basis: {ctd_net_basis:.6f}")
  # print(f"Implied Repo Rate: {implied_repo_rate:.6f}")
  # print(f"Convexity-Adjusted Yield: {convexity_yield:.6f}")
