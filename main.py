import concurrent.futures
import threading
import time

from Future_index import Future
from business_logic import business_logic_function
from scraper import run_scraper  # Import the new scraper function


def run_fut_discovery():
    """
    Initializes a Future instance and triggers the discovery process for Futures.
    This function is meant to be executed as a separate task.
    """
    futures_instance = Future()
    futures_instance.discover()


def population_function():
    """
    Continuously runs Future discovery in a loop (UST scan has been moved to run once at startup).
    This function runs in a loop, submitting tasks to a ThreadPoolExecutor every 2 seconds.
    """
    while True:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(run_fut_discovery)
        time.sleep(2)


if __name__ == "__main__":
    """
    Main execution point of the script.
    - Runs the scraper once before starting background tasks.
    - Starts threads for Future discovery and business logic.
    - Uses daemon threads so they stop when the main program exits.
    """
    print("🚀 Running initial UST scraper...")
    run_scraper()  # Run the scraper once at startup

    # Run background threads for population and business logic
    population_function_thread = threading.Thread(target=population_function, daemon=True)
    business_logic_function_thread = threading.Thread(target=business_logic_function, daemon=True)

    population_function_thread.start()
    business_logic_function_thread.start()

    # Keep the main thread alive to prevent script termination
    while True:
        time.sleep(1)
