# Configuration settings for bond trading application
import pandas as pd

# IBKR Client Portal Web API
IBKR_BASE_URL = "https://localhost:5000"
IBKR_ACCT_ID = ""  # populate with IBKR Acct ID. Leave empty for security.

# Logging Settings
LOG_FORMAT = "'%(asctime)s - %(name)s - %(levelname)s - %(message)s'"
LOG_LEVEL = "INFO"
LOG_FILE = "application.log"

# Applicable Objects
FUT_SYMBOLS = "ZT,ZF,ZN,TN,Z3N"
UST_SIZE = 25  # Number of applicable distinct US-T to filter from IBKR scanner
USTs = pd.DataFrame()  # DF to be populated during runtime
FUTURES = pd.DataFrame()  # DF to be populated during runtime
HEDGES = pd.DataFrame()  # DF to be populated during runtime
HEDGES_Combos = pd.DataFrame()  # DF to be populated during runtime
ORDERS = pd.DataFrame()  # DF to be populated during runtime
file_path = "C:\trade\fut_frm_blocking\bona_fide_hedging\treasury-futures-conversion-factor-look-up-tables.xlsm"
X = 0
