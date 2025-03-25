# Configuration settings for bond trading application
import pandas as pd

# IBKR Client Portal Web API
IBKR_BASE_URL = "https://localhost:5000"
IBKR_ACCT_ID = "3297612"  # populate with IBKR Acct ID. Leave empty for security.

# Logging Settings
LOG_FORMAT = "'%(asctime)s - %(name)s - %(levelname)s - %(message)s'"
LOG_LEVEL = "INFO"
LOG_FILE = "application.log"

# Applicable Objects
FUT_SYMBOLS = "ZT,ZF,ZN,TN,Z3N"
USTs = pd.DataFrame()  # DF to be populated during runtime
FUTURES = pd.DataFrame()  # DF to be populated during runtime
HEDGES = pd.DataFrame()  # DF to be populated during runtime
HEDGES_Combos = pd.DataFrame()  # DF to be populated during runtime
ORDERS = pd.DataFrame()  # DF to be populated during runtime
X = 0
ACTIVE_ORDERS_LIMIT = 5  # Limit for number of active orders per time