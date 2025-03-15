import logging
import random
import sys
from datetime import timedelta, datetime

import pandas as pd
import requests
import urllib3

import config
from enums.USTMarketDataField import USTMarketDataField
from market_data import MarketData
from contract import Contract
from leaky_bucket import leaky_bucket

# Configure logging to both file and stdout
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT, handlers=[
    logging.FileHandler(config.LOG_FILE),
    logging.StreamHandler(sys.stdout)
])

# Disable SSL Warnings (for external API requests)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class UST:
    def __init__(self):
        """ Initializes the UST class with an empty list for storing U.S. Treasury securities. """
        self.USTs_List = []  # List to hold scanned USTs

    def scan(self):
        """
        Performs the UST index scan by:
        - Defining a date range for securities (up to 10 years to maturity at issuance).
        - Scanning securities within the date range.
        - Fetching security definitions and extracting relevant contract fields.
        - Requesting and processing market data.
        - Converting the results to a Pandas DataFrame and computing additional KPIs.
        The final result is stored in `config.USTs`.
        """

        # Define maturity date range (730 days to 3651 days from today)
        start_date = datetime.now() + timedelta(days=730)
        end_date = datetime.now() + timedelta(days=3651)  # USTs must have <10 years to maturity at issuance

        # Scan for securities within the defined date range
        scanned_securities = self.scan_securities(start_date, end_date, config.UST_SIZE)

        logging.info(f'Fetching UST security definitions for {len(scanned_securities)} securities...')
        security_definitions = Contract.get_security_definition(scanned_securities)

        logging.info(f'Extracting relevant fields from security definitions...')
        self.USTs_List = Contract.extract_contract_fields(security_definitions)

        logging.info('Requesting market data for matched USTs...')
        ust_market_data = MarketData.get_market_data(self.USTs_List, USTMarketDataField)

        # Extract relevant market data fields
        self.USTs_List = MarketData.extract_market_data_fields(self.USTs_List, ust_market_data)

        # Convert to a Pandas DataFrame for further processing
        config.USTs = pd.DataFrame(self.USTs_List)
        logging.info('UST scan and population completed.')

    def scan_securities(self, start_date, end_date, size):
        """
        Scans and retrieves a list of UST securities within a given maturity date range.
        The function repeatedly calls `scan_ust()` until the required number of securities (`size`) is gathered.
        Parameters:
        - `start_date` (datetime): The earliest maturity date allowed.
        - `end_date` (datetime): The latest maturity date allowed.
        - `size` (int): The number of UST securities to fetch.
        Returns:
        - list: A list of UST securities.
        """
        while len(self.USTs_List) < size:
            self.scan_ust(start_date, end_date)
        return self.USTs_List

    def scan_ust(self, start_date, end_date):
        """
        Scans for U.S. Treasury bonds within a specified maturity date range by:
        - Generating a randomized start date.
        - Sending a request to the IBKR API.
        - Waiting for an API rate limit token (via a leaky bucket).
        - Processing the API response and extracting contract data.
        Parameters:
        - `start_date` (datetime): The minimum maturity date allowed.
        - `end_date` (datetime): The maximum maturity date allowed.
        Returns:
        - None (updates `self.USTs_List` with valid contracts).
        """

        # Ensure the randomized `start_date` maintains a minimum 30-day gap from `end_date`
        min_start = start_date
        max_start = end_date - timedelta(days=30)

        if min_start >= max_start:
            randomized_start_date = min_start  # Use fallback if the date range is too small
        else:
            randomized_start_date = min_start + timedelta(days=random.randint(0, (max_start - min_start).days))

        request_payload = {
            "instrument": "BOND.GOVT",
            "location": "BOND.GOVT.US",
            "type": "BOND_CUSIP_AZ",
            "filter": [
                {"code": "maturityDateAbove", "value": randomized_start_date.strftime("%Y%m%d")},
                {"code": "maturityDateBelow", "value": end_date.strftime("%Y%m%d")},
            ]
        }

        url = f"{config.IBKR_BASE_URL}/v1/api/iserver/scanner/run"
        logging.info(f'Requesting from {url}: {request_payload}')

        # Wait until an API request token is available (rate-limiting control)
        leaky_bucket.wait_for_token()

        # Make the request
        response = requests.post(url, json=request_payload, verify=False)
        if response.status_code != 200 or not response.json().get("contracts"):
            logging.error(f'Failed to retrieve contracts from {url}: HTTP {response.status_code}')
            return

        contracts = response.json().get("contracts")
        logging.info(f'Successfully retrieved {len(contracts)} contracts.')

        # Process and filter contracts
        self.filter_us_treasury_bonds(contracts)

    def filter_us_treasury_bonds(self, contracts):
        """
        Filters valid U.S. Treasury bonds from a given list of contract data.
        The function removes:
        - Treasury Inflation-Protected Securities (TIPS)
        - STRIPS (Separate Trading of Registered Interest and Principal Securities)
        - Treasury Bills (short-term securities)
        - Floating Rate Notes (FRNs) and When-Issued Floating Rate Notes
        Valid U.S. Treasury bonds:
        - Must have 'govt' in the description.
        - Must have the symbol 'US-T'.
        - Must not already exist in `USTs_List` (prevents duplicates).
        Parameters:
        - `contracts` (list): A list of contract dictionaries.
        Returns:
        - None (modifies `self.USTs_List` in place).
        """
        for item in contracts:
            contract_desc = str(item.get("contract_description_2", "")).lower()
            symbol = item.get("symbol", "")
            con_id = item.get("con_id")  # Unique identifier

            # Exclude non-standard Treasury securities
            if ("tips" in contract_desc or "strips" in contract_desc or "bill" in contract_desc
                    or "floating" in contract_desc):
                continue

            # Add valid USTs if they meet criteria and are not duplicates
            if ("govt" in contract_desc) and symbol == 'US-T':
                if not any(existing.get("con_id") == con_id for existing in self.USTs_List):
                    self.USTs_List.insert(0, item)
                    logging.info(f'Added a new UST contract. Total count: {len(self.USTs_List)}')
