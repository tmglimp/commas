import logging
import sys
from datetime import datetime

import pandas as pd
import requests
import urllib3

import config
from contract import Contract
from enums.FutContractField import FutContractField
from enums.FutMarketDataField import FutMarketDataField
from fixed_income_calc import compute_settlement_date, calculate_term
from leaky_bucket import leaky_bucket
from market_data import MarketData

# Configure logging to both file and stdout
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT, handlers=[
    logging.FileHandler(config.LOG_FILE),
    logging.StreamHandler(sys.stdout)
])

# Disable SSL Warnings (Against Client Web API)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Future:

    def __init__(self):
        self.FUTURES_List = []  # List containing all scanned Futures

    def discover(self):
        """
        Discover and populate futures based on the given symbols in the configuration.

        This function will:
        1. Fetch futures symbols.
        2. Filter futures by expiration date.
        3. Retrieve contract security definitions.
        4. Fetch market data for futures contracts.
        5. Merge contract data with market data and populate the `config.FUTURES` DataFrame.
        """
        fut_symbols_csv = config.FUT_SYMBOLS  # CSV of applicable symbols

        logging.info(f'Discovering futures for {fut_symbols_csv}')

        # Go ahead and fetch security futures based on the provided symbols
        futures = self.scan(fut_symbols_csv)
        futures = self.extract_futures_contracts(futures)
        self.FUTURES_List = self.filter_futures_by_expiry(futures, 2)

        logging.info(f'Discovered a total of {len(self.FUTURES_List)} futures securities for risk management.')

        logging.info(f'Getting contract definition details for {len(self.FUTURES_List)} futures securities...')
        security_definitions = Contract.get_security_definition(self.FUTURES_List)
        if not security_definitions:
            logging.error('Unable to fetch contract security definitions.')
            return

        logging.info(f'Getting live market data for {len(self.FUTURES_List)} futures securities...')
        fut_market_data = MarketData.get_market_data(self.FUTURES_List, FutMarketDataField)
        if not fut_market_data:
            logging.error('Unable to fetch market data.')
            return

        # Merge both fut sec def details & market_data to form a super array for the futures contracts
        futures_data = self.extract_contract_market_data_fields(security_definitions, fut_market_data)

        # Convert to DataFrame for easy computation application
        config.FUTURES = pd.DataFrame(futures_data)
        config.FUTURES = self.update_empty_price(config.FUTURES)

        logging.info('Converting Futures price to decimal...')
        config.FUTURES = self.convert_price_to_decimal(config.FUTURES)

        logging.info('Finished Futures population.')
        logging.info('Fetching futures details...')

        # Retrieve security futures
        url = config.IBKR_BASE_URL + f"/v1/api/trsrv/futures?symbols={symbols_csv}"

        # Block until a token is available
        leaky_bucket.wait_for_token()

        logging.info(f'Requesting from {url}')

        response = requests.get(url=url, verify=False)

        if response.status_code != 200:
            logging.error(f'Response from {url}: {response.status_code} : Unable to proceed '
                          f'with security futures retrieval.')
            return None

        logging.info(f'Response from {url}: {response.status_code} : Successfully retrieved security futures')

        return response.json()
    @staticmethod
    def scan(symbols_csv):
        """
        Fetch security futures for the provided comma-separated symbols list.
        Uses the IBKR /trsrv/futures endpoint.
        """
        url = f"{config.IBKR_BASE_URL}/v1/api/trsrv/futures?symbols={symbols_csv}"

        # Wait for a token using the leaky bucket
        leaky_bucket.wait_for_token()

        try:
            logging.info(f"📡 Requesting futures from: {url}")
            response = requests.get(url, verify=False)

            if response.status_code != 200:
                logging.error(f"❌ Failed to fetch futures. Status {response.status_code}")
                return {}

            logging.info("✅ Successfully fetched futures.")
            return response.json()

        except Exception as e:
            logging.exception(f"❌ Exception occurred while fetching futures: {e}")
            return {}


    @staticmethod
    def extract_futures_contracts(futures):
        """
        Flatten nested futures data into a list of contracts.

        The response for futures from IBKR comes in a nested format per symbol.
        This function flattens that data into a single list of contracts for easier processing.
        """
        extracted_futures = []
        if futures is not None:
            extracted_futures = [contract for contracts in futures.values() for contract in contracts]
        return extracted_futures

    @staticmethod
    def extract_contract_market_data_fields(contract_details, market_data):
        """
        Merges contract details with corresponding market data fields based on contract `con_id`.

        This function iterates through the given contract details and market data, matching
        them by `con_id`. It combines contract-specific information (e.g., currency, ticker,
        expiry) with market data (e.g., bid/ask prices, volume, implied volatility) and computes
        the year to maturity for each contract based on the settlement date. The resulting merged
        information is returned as a list of dictionaries.
        """
        contracts = []

        logging.info("Collating contract and market data details: Started...")

        for contract in contract_details:
            contract_info = {
                FutContractField.con_id.name: contract.get(FutContractField.con_id.value),
                FutContractField.currency.name: contract.get(FutContractField.currency.value),
                FutContractField.ticker.name: contract.get(FutContractField.ticker.value),
                FutContractField.full_name.name: contract.get(FutContractField.full_name.value),

                FutContractField.all_exchanges.name: contract.get(FutContractField.all_exchanges.value),
                FutContractField.listing_exchanges.name: contract.get(FutContractField.listing_exchanges.value),
                FutContractField.asset_class.name: contract.get(FutContractField.asset_class.value),
                FutContractField.expiry.name: contract.get(FutContractField.expiry.value),
                FutContractField.last_trading_day.name: contract.get(FutContractField.last_trading_day.value),
                FutContractField.strike.name: contract.get(FutContractField.strike.value),
                FutContractField.underlying_conid.name: contract.get(FutContractField.underlying_conid.value),
                FutContractField.underlying_exchange.name: contract.get(FutContractField.underlying_exchange.value),
                FutContractField.multiplier.name: contract.get(FutContractField.multiplier.value),

                FutContractField.increment.name: next(iter(contract.get('incrementRules', {})), {}).get(
                    FutContractField.increment.value),

                FutContractField.increment_lower_edge.name: next(iter(contract.get('incrementRules', {})), {}).get(
                    FutContractField.increment_lower_edge.value)
            }

            trade_settle_date = compute_settlement_date(datetime.today().strftime('%Y%m%d'))
            contract_info[FutContractField.year_to_maturity.name] = (
                calculate_term(str(trade_settle_date), contract_info[FutContractField.expiry.name]))

            for data in market_data:

                if contract.get(FutContractField.con_id.value) == data.get(FutContractField.con_id.value):
                    contract_info[FutMarketDataField.symbol.name] = data.get(str(FutMarketDataField.symbol.value))
                    contract_info[FutMarketDataField.daily_pnl.name] = data.get(str(FutMarketDataField.daily_pnl.value))
                    contract_info[FutMarketDataField.realized_pnl.name] = data.get(
                        str(FutMarketDataField.realized_pnl.value))
                    contract_info[FutMarketDataField.ask_price.name] = data.get(str(FutMarketDataField.ask_price.value))
                    contract_info[FutMarketDataField.ask_size.name] = data.get(str(FutMarketDataField.ask_size.value))
                    contract_info[FutMarketDataField.bid_price.name] = data.get(str(FutMarketDataField.bid_price.value))
                    contract_info[FutMarketDataField.bid_size.name] = data.get(str(FutMarketDataField.bid_size.value))
                    contract_info[FutMarketDataField.last_price.name] = data.get(
                        str(FutMarketDataField.last_price.value))
                    contract_info[FutMarketDataField.last_size.name] = data.get(str(FutMarketDataField.last_size.value))
                    contract_info[FutMarketDataField.volume.name] = data.get(str(FutMarketDataField.volume.value))
                    contract_info[FutMarketDataField.right.name] = data.get(str(FutMarketDataField.right.value))
                    contract_info[FutMarketDataField.exchange.name] = data.get(str(FutMarketDataField.exchange.value))
                    contract_info[FutMarketDataField.months.name] = data.get(str(FutMarketDataField.months.value))
                    contract_info[FutMarketDataField.regular_expiry.name] = data.get(
                        str(FutMarketDataField.regular_expiry.value))
                    contract_info[FutMarketDataField.underlying_conid.name] = data.get(
                        str(FutMarketDataField.underlying_conid.value))
                    contract_info[FutMarketDataField.market_data_availability.name] = data.get(
                        str(FutMarketDataField.market_data_availability.value))
                    contract_info[FutMarketDataField.ask_exch.name] = data.get(str(FutMarketDataField.ask_exch.value))
                    contract_info[FutMarketDataField.last_exch.name] = data.get(str(FutMarketDataField.last_exch.value))
                    contract_info[FutMarketDataField.bid_exch.name] = data.get(str(FutMarketDataField.bid_exch.value))
                    contract_info[FutMarketDataField.implied_vol.name] = data.get(
                        str(FutMarketDataField.implied_vol.value))
                    contract_info[FutMarketDataField.option_volume.name] = data.get(
                        str(FutMarketDataField.option_volume.value))
                    contract_info[FutMarketDataField.conid_exchange.name] = data.get(
                        str(FutMarketDataField.conid_exchange.value))

            contracts.append(contract_info)

        logging.info("Collating contract and market data details: Completed.")
        return contracts

    @staticmethod
    def filter_futures_by_expiry(futures, year_to_maturity=2):
        f"""
        This function ensures only futures with expiry under {year_to_maturity} are being used.
        """
        # Compute settlement date
        trade_settle_date = compute_settlement_date(datetime.today().strftime('%Y%m%d'))

        # Filter futures with year to maturity under 2 years
        filtered_futures = [
            future for future in futures if
            calculate_term(str(trade_settle_date), str(future['expirationDate'])) < year_to_maturity
        ]

        return filtered_futures

    @staticmethod
    def update_empty_price(df):
        """
        Fill missing ask_price and bid_price with last_price if market is closed and price data isn't available
        """
        #
        df["ask_price"] = df["ask_price"].fillna(df["last_price"].str.lstrip("C"))
        df["bid_price"] = df["bid_price"].fillna(df["last_price"].str.lstrip("C"))

        return df

    def convert_price_to_decimal(self, df):
        """
        Futures prices are quoted in 32nds of a point.
        This function basically just converts price quotes to the decimal representation.
        """

        fut_type = {
            '0.00390625': 'half',
            '0.015625': 'quarter',
            '0.03125': 'eighth',
            'sixteenth': 'sixteenth'
        }
        df["ask_price_decimal"] = df["ask_price"].apply(
            lambda x: self.convert_futures_price(x if x not in [None, ""] else 0, fut_type.get(str(df["increment"])))
        )
        df["bid_price_decimal"] = df["bid_price"].apply(
            lambda x: self.convert_futures_price(x if x not in [None, ""] else 0, fut_type.get(str(df["increment"])))
        )
        df["spread_decimal"] = df.apply(
            lambda row: self.calculate_spread(row["ask_price_decimal"], row["bid_price_decimal"]),
            axis=1)
        df["price"] = df.apply(
            lambda row: self.calculate_mid_price(row["ask_price_decimal"], row["bid_price_decimal"]),
            axis=1)

        return df

    @staticmethod
    def convert_futures_price(price: str, fut_type: str) -> float:
        """
        Convert futures price in 1/2s, 1/4s, 1/8ths, and 1/16ths of a 32nd to decimal dollars.
        Example input: '134'16.5' (which means 134 16.5/32)
        """
        try:
            if "'" not in str(price):
                return float(price)

            # Split the price into the whole number and fraction part
            whole, fraction = price.split("'")
            whole = int(whole)
            # Determine the fraction denominator based on fut_type
            denominators = {
                'half': 2,
                'quarter': 4,
                'eighth': 8,
                'sixteenth': 16
            }
            denominator = denominators.get(fut_type, 32)
            # Handle fraction part
            if '.' in fraction:  # If there's a decimal (fraction of a 32nd)
                frac_32nd, frac_part = map(int, fraction.split('.'))
                decimal_fraction = frac_32nd / 32 + frac_part / (32 * denominator)
            else:
                decimal_fraction = int(fraction) / 32
            return whole + decimal_fraction
        except ValueError:
            raise ValueError("Invalid futures price format. Expected format: 134'16.5")

    @staticmethod
    def calculate_spread(ask_decimal, bid_decimal):
        """Calculate spread between ask and bid prices."""
        return ask_decimal - bid_decimal  # Spread in decimal format

    @staticmethod
    def calculate_mid_price(ask_decimal, bid_decimal):
        """Calculate mid-price between ask and bid prices."""
        return ask_decimal + bid_decimal / 2
