import concurrent.futures
import threading
import time

from Future_index import Future
from UST_index import UST
from business_logic import business_logic_function


def run_ust_scan():
    """
    Initializes a UST instance and triggers the scan process for U.S. Treasuries.
    This function is meant to be executed as a separate task.
    """
    ust_instance = UST()
    ust_instance.scan()


def run_fut_discovery():
    """
    Initializes a Future instance and triggers the discovery process for Futures.
    This function is meant to be executed as a separate task.
    """
    futures_instance = Future()
    futures_instance.discover()


def population_function():
    """
    Continuously runs UST scanning and Future discovery in parallel.
    This function runs in a loop, submitting tasks to a ThreadPoolExecutor
    to run asynchronously every 2 seconds.
    """
    while True:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(run_ust_scan)
            executor.submit(run_fut_discovery)
        time.sleep(2)  # Wait 2 seconds before the next iteration


if __name__ == "__main__":
    """
    Main execution point of the script.
    - Starts two threads: one for UST/Future discoveries and another for business logic.
    - Uses daemon threads so they stop when the main program exits.
    - Keeps the main thread alive indefinitely.
    """
    # Run both functions in separate threads concurrently
    population_function_thread = threading.Thread(target=population_function, daemon=True)
    business_logic_function_thread = threading.Thread(target=business_logic_function, daemon=True)

    population_function_thread.start()
    business_logic_function_thread.start()

    # Keep the main thread alive to prevent script termination
    while True:
        time.sleep(1)
