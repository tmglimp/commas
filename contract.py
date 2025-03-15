import logging
import sys
from datetime import datetime

import requests
import urllib3

import config
from fixed_income_calc import compute_settlement_date, calculate_term
from enums.USTContractField import USTContractField
from leaky_bucket import leaky_bucket

# Configure logging to both file and stdout
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT, handlers=[
    logging.FileHandler(config.LOG_FILE),
    logging.StreamHandler(sys.stdout)
])

# Disable SSL Warnings (Against Client Web API)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Contract:

    @staticmethod
    def get_security_definition(contracts, batch_size=50):
        """
        Fetch contract details for corresponding array list [] of contracts.
        """

        # Request contract details for all contracts collected
        contract_details = []
        logging.info('Fetching contract details...')
        for i in range(0, len(contracts), batch_size):
            batch_contracts = contracts[i:i + batch_size]
            csv_con_ids = ",".join(str(item.get("con_id", item.get("conid"))) for item in batch_contracts)

            url = config.IBKR_BASE_URL + f"/v1/api/trsrv/secdef?conids={csv_con_ids}"

            # Block until a token is available
            leaky_bucket.wait_for_token()

            logging.info(f'Requesting from {url}')

            response = requests.get(url=url, verify=False)

            if response.status_code != 200:
                logging.error(f'Response from {url}: {response.status_code} : Unable to proceed '
                              f'with contract details retrieval.')
                continue  # Skip to the next batch on error

            logging.info(f'Response from {url}: {response.status_code} : Successfully retrieved contract details')

            contract_details.extend(response.json()["secdef"])  # Add details to the main list

        logging.info('Done fetching contract details...')

        return contract_details

    @staticmethod
    def extract_contract_fields(contract_details):

        contracts = []

        logging.info("Extracting contract details fields: Started...")

        for contract in contract_details:
            contract_info = {
                USTContractField.con_id.name: contract.get(USTContractField.con_id.value),
                USTContractField.currency.name: contract.get(USTContractField.currency.value),
                USTContractField.ticker.name: contract.get(USTContractField.ticker.value),
                USTContractField.bond_name.name: contract.get(USTContractField.bond_name.value),
                USTContractField.full_name.name: contract.get(USTContractField.full_name.value),
                USTContractField.desc_label.name: contract['bond'].get(USTContractField.desc_label.value),
                USTContractField.country_of_issue.name: contract['bond'].get(USTContractField.country_of_issue.value),
                USTContractField.principal_value.name: contract['bond'].get(USTContractField.principal_value.value),
                USTContractField.issue_date.name: contract['bond'].get(USTContractField.issue_date.value),
                USTContractField.maturity_date.name: contract['bond'].get(USTContractField.maturity_date.value),
                USTContractField.pay_principal_on_maturity.name: contract['bond'].get(
                    USTContractField.pay_principal_on_maturity.value),
                USTContractField.bb_mkt_lss.name: contract['bond'].get(USTContractField.bb_mkt_lss.value),

                # There is possibility of coupon not being present, not present or present but having an empty array
                USTContractField.coupon_rate.name: next(iter(contract.get('bond', {}).get('coupon', [{}])), {}).get(
                    USTContractField.coupon_rate.value),
                USTContractField.coupon_first_date.name: next(iter(contract.get('bond', {}).get('coupon', [{}])), {}).get(
                    USTContractField.coupon_first_date.value),
                USTContractField.coupon_first_accr_date.name: next(iter(contract.get('bond', {}).get('coupon', [{}])),
                                                                {}).get(
                    USTContractField.coupon_first_accr_date.value),
                USTContractField.coupon_prev_date.name: next(iter(contract.get('bond', {}).get('coupon', [{}])), {}).get(
                    USTContractField.coupon_prev_date.value),
                USTContractField.coupon_second_date.name: next(iter(contract.get('bond', {}).get('coupon', [{}])), {}).get(
                    USTContractField.coupon_second_date.value),
                USTContractField.coupon_ncpdt.name: next(iter(contract.get('bond', {}).get('coupon', [{}])), {}).get(
                    USTContractField.coupon_ncpdt.value),
                USTContractField.coupon_cpc.name: next(iter(contract.get('bond', {}).get('coupon', [{}])), {}).get(
                    USTContractField.coupon_cpc.value),
                USTContractField.si_id.name: next(iter(contract.get('bond', {}).get('si', [{}])), {}).get(
                    USTContractField.si_id.value),

                USTContractField.increment.name: next(iter(contract.get('incrementRules', {})), {}).get(
                    USTContractField.increment.value),

                USTContractField.increment_lower_edge.name: next(iter(contract.get('incrementRules', {})), {}).get(
                    USTContractField.increment_lower_edge.value),

                USTContractField.issue_amount.name: contract['bond'].get(USTContractField.issue_amount.value),
                USTContractField.open_amount.name: contract['bond'].get(USTContractField.open_amount.value),
                USTContractField.initial_price.name: contract['bond'].get(USTContractField.initial_price.value)
            }

            si_id = contract_info.get(USTContractField.si_id.name, "")
            contract_info[USTContractField.cusip.name] = si_id[2:-1] if si_id and si_id[0].isalpha() else si_id

            trade_settle_date = compute_settlement_date(datetime.today().strftime('%Y%m%d'))
            contract_info[USTContractField.year_to_maturity.name] = (
                calculate_term(str(trade_settle_date), contract_info[USTContractField.maturity_date.name]))

            contracts.append(contract_info)

        logging.info("Extracting contract details fields: Finished.")

        return contracts
