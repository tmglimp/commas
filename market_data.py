import logging
import sys

import requests
import urllib3

import config
from enums.USTContractField import USTContractField
from enums.USTMarketDataField import USTMarketDataField
from leaky_bucket import leaky_bucket

# Configure logging to both file and stdout
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT, handlers=[
    logging.FileHandler(config.LOG_FILE),
    logging.StreamHandler(sys.stdout)
])

# Disable SSL Warnings (Against Client Web API)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MarketData:

    @staticmethod
    def get_market_data(contracts, fields, batch_size=50):
        """
        Fetches market data for a list of contracts in batches.

        Args:
            contracts: A list of dictionaries representing contracts.
            fields: The market data enum type to be useds.
            batch_size: The number of contracts to process in each API call (default 50).

        Returns:
            An array containing market data for all contracts.
        """

        market_data = []  # Array to store all market data

        # Get the market data fields we are interested in
        fields = [field.value for field in fields]

        logging.info('Fetching market data...')
        for i in range(0, len(contracts), batch_size):
            batch_contracts = contracts[i:i + batch_size]
            csv_con_ids = ",".join(str(item.get("con_id", item.get("conid"))) for item in batch_contracts)
            csv_fields = ",".join(map(str, fields))

            url = config.IBKR_BASE_URL + (f"/v1/api/iserver/marketdata/snapshot?"
                                          f"conids={csv_con_ids}&fields={csv_fields}&live=true")

            # Block until a token is available
            leaky_bucket.wait_for_token()

            logging.info(f'Requesting from {url}')

            response = requests.get(url=url, verify=False)

            if response.status_code != 200:
                logging.error(f'Response from {url}: {response.status_code} : Unable to fetch market data.')
                continue  # Skip to the next batch on error

            logging.info(f'Response from {url}: {response.status_code} : Successfully scanned market data.')

            # Update the market_data array based on response
            market_data.extend(response.json())  # Merge data from each batch

        logging.info('Done fetching market data...')

        return market_data

    @staticmethod
    def extract_market_data_fields(contract_details, market_data):
        """
        Extracts and collates market data fields from a list of contract details and
        market data. It matches contracts based on `con_id` and adds various market
        data fields (e.g., symbol, price, yield, volume) to the corresponding contract.

        Parameters:
        - `contract_details` (list): A list of contract dictionaries.
        - `market_data` (list): A list of market data dictionaries.

        Returns:
        - list: A list of contracts with updated market data fields.
        """

        contracts = []

        logging.info("Collating contract and market dat details: Started...")

        for contract in contract_details:

            for data in market_data:

                if contract.get(USTContractField.con_id.name) == data.get(USTContractField.con_id.value):
                    contract[USTMarketDataField.symbol.name] = data.get(str(USTMarketDataField.symbol.value))
                    contract[USTMarketDataField.text.name] = data.get(str(USTMarketDataField.text.value))
                    contract[USTMarketDataField.last_price.name] = data.get(str(USTMarketDataField.last_price.value))
                    contract[USTMarketDataField.bid_size.name] = data.get(str(USTMarketDataField.bid_size.value))
                    contract[USTMarketDataField.ask_size.name] = data.get(str(USTMarketDataField.ask_size.value))
                    contract[USTMarketDataField.last_size.name] = data.get(str(USTMarketDataField.last_size.value))
                    contract[USTMarketDataField.last_yield.name] = data.get(str(USTMarketDataField.last_yield.value))
                    contract[USTMarketDataField.last_exch.name] = data.get(str(USTMarketDataField.last_exch.value))
                    contract[USTMarketDataField.avg_price.name] = data.get(str(USTMarketDataField.avg_price.value))
                    contract[USTMarketDataField.bid_price.name] = data.get(str(USTMarketDataField.bid_price.value))
                    contract[USTMarketDataField.bid_yield.name] = data.get(str(USTMarketDataField.bid_yield.value))
                    contract[USTMarketDataField.bid_exch.name] = data.get(str(USTMarketDataField.bid_exch.value))
                    contract[USTMarketDataField.ask_price.name] = data.get(str(USTMarketDataField.ask_price.value))
                    contract[USTMarketDataField.ask_yield.name] = data.get(str(USTMarketDataField.ask_yield.value))
                    contract[USTMarketDataField.ask_exch.name] = data.get(str(USTMarketDataField.ask_exch.value))
                    contract[USTMarketDataField.volume.name] = data.get(str(USTMarketDataField.volume.value))
                    contract[USTMarketDataField.avg_volume.name] = data.get(str(USTMarketDataField.avg_volume.value))
                    contract[USTMarketDataField.exchange.name] = data.get(str(USTMarketDataField.exchange.value))
                    contract[USTMarketDataField.marker.name] = data.get(str(USTMarketDataField.marker.value))
                    contract[USTMarketDataField.underlying_conid.name] = data.get(
                        str(USTMarketDataField.underlying_conid.value))
                    contract[USTMarketDataField.mkt_data_avail.name] = data.get(
                        str(USTMarketDataField.mkt_data_avail.value))
                    contract[USTMarketDataField.company.name] = data.get(str(USTMarketDataField.company.value))
                    contract[USTMarketDataField.contract_description.name] = data.get(
                        str(USTMarketDataField.contract_description.value))
                    contract[USTMarketDataField.listing_exchange.name] = data.get(
                        str(USTMarketDataField.listing_exchange.value))
                    contract[USTMarketDataField.shortable_shares.name] = data.get(
                        str(USTMarketDataField.shortable_shares.value))
                    contract['price'] = (
                        (float(contract[USTMarketDataField.ask_price.name]) + float(contract[
                            USTMarketDataField.bid_price.name])) / 2.0
                        if contract[USTMarketDataField.ask_price.name] and contract[USTMarketDataField.bid_price.name]
                        else None
                    )

                    # Convert ask_yield and bid_yield to numeric
                    contract[USTMarketDataField.ask_yield.name] = (
                        float(contract[USTMarketDataField.ask_yield.name].rstrip("%")))
                    contract[USTMarketDataField.bid_yield.name] = (
                        float(contract[USTMarketDataField.bid_yield.name].rstrip("%")))

                    # Compute average and create 'yield' column
                    contract["yield"] = (contract[USTMarketDataField.ask_yield.name] +
                                         contract[USTMarketDataField.bid_yield.name] / 2.0)

                    contracts.append(contract)
                    break

        logging.info("Collating contract and market dat details: Finished.")

        return contracts
