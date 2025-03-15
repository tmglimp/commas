"""
Orders
"""
import json

import requests
import urllib3

import config
from leaky_bucket import leaky_bucket

# Ignore insecure error messages
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def orderRequest(ORDERS):
    # Fetching the data from ORDERS
    # Using top_row for now. We need to reconfirm parameters again.
    front_conId = int(ORDERS[0]["front_conId"])
    front_ratio = round(ORDERS[0]["front_ratio"])
    back_conId = int(ORDERS[0]["back_conId"])
    back_ratio = round(ORDERS[0]["back_ratio"])
    quantity = int(ORDERS[0]["quantity"])  # Convert explicitly
    price = round(ORDERS[0]["price"], 5)  # To be fixed

    url = f"{config.IBKR_BASE_URL}/v1/api/iserver/account/{config.IBKR_ACCT_ID}/orders"

    # Constructing the order JSON body
    json_body = {
        "orders": [
            {
                "conidex": f"28812380;;;{front_conId}/{front_ratio},{back_conId}/{back_ratio}",
                "orderType": "LMT",
                "price": price,
                "side": "BUY",
                "tif": "DAY",
                "quantity": quantity
            }
        ]
    }

    # Wait until an API request token is available (rate-limiting control)
    leaky_bucket.wait_for_token()

    print(f'Placing order: {url}')
    print(json.dumps(json_body, indent=2))  # Pretty-print for debugging

    # Sending the POST request
    order_req = requests.post(url=url, verify=False, json=json_body)

    print(order_req.status_code)
    print(order_req.text)  # Print response body for debugging


if __name__ == "__main__":
    orderRequest(config.ORDERS)
