import time

from KPIs2_Orders import calculate_quantities_with_sma
import config
from leaky_bucket_orders import OrdersLeakyBucket
from orders import orderRequest
from cf_ctd import process_futures_data
from ctd_fut_kpis import run_fixed_income_calculation

# Initialize the leaky bucket for orders
leaky_bucket_orders = OrdersLeakyBucket()


def business_logic_function():
    """
    Continuously executes the business logic as a separate process all the way to the order placement.
    This function runs in a loop and executes every 3 seconds.
    """
    while True:
        # Ensure that the config.USTs and config.FUTURES DataFrames exist and are neither None nor empty.
        # config.USTs and config.FUTURES are being populated continuously from another thread.
        # IF any other script modifies the config.* objects, business_logic.py will always have the latest version.
        if config.USTs is not None and config.FUTURES is not None and not config.USTs.empty and not config.FUTURES.empty:
            print("****Business logic started, leveraging other integrated scripts***")

            leaky_bucket_orders.wait_for_slot()  # Wait until there's an available slot for orders

            print("Scanned USTs:")
            print(config.USTs)

            print("Scanned FUTURES:")
            print(config.FUTURES)

            process_futures_data(config.FUTURES, config.USTs, config.file_path)  # From cf_ctd.py
            print("Populated HEDGES:")
            print(config.HEDGES)  # results object

            run_fixed_income_calculation(config.HEDGES)  # From ctd_fut_kpis.py
            print("Updated HEDGES:")
            print(config.HEDGES)

            print("Populated HEDGES_Combos:")
            print(config.HEDGES_Combos)  # results object

            calculate_quantities_with_sma(config.HEDGES_Combos)  # From KPIs2_Orders.py
            print("Updated HEDGES_Combos:")
            print(config.HEDGES_Combos)

            print("Populated ORDERS:")
            print(config.ORDERS)  # results object

            orderRequest(config.ORDERS)  # From orders.py

        time.sleep(.000001)  # Wait .000001 seconds before the next iteration
