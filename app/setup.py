import os

POSTGRES_USER='postgres-user-here'
POSTGRES_PASSWORD='postgres-password-here'
POSTGRES_DB='exacaster'
PORT='5435'
SQLALCHEMY_CON_STR = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{PORT}/{POSTGRES_DB}"

CHUNK_SIZE = 500000
DELETE_AFTER_DAYS = 180
DATA_FILE = F"{os.getcwd()}/usage.csv"
ERROR_FILE_PATH = os.getcwd()
COLUMN_NAMES = [
    "customer_id",
    "event_start_time",
    "service_type",
    "rate_plan_id",
    "billing_flag_1",
    "billing_flag_2",
    "duration",
    "charge",
    "month",
]
INTEGER_COLUMNS = [
    "customer_id",
    "rate_plan_id",
    "customer_id",
    "billing_flag_1",
    "billing_flag_2",
]
DATE_COLUMNS = ["event_start_time"]
CURRENCY_COLUMNS = ["charge"]