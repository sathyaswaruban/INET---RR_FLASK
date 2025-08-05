import os
import logging
import datetime

# Get today's date parts
today = datetime.date.today()
year_str = str(today.year)
month_str = f"{today.month:02d}"
day_str = f"{today.day:02d}"

# Directory: logs/YYYY/MM/
log_dir = os.path.join("D:/INET_RR_FLASK/logs", year_str, month_str)
os.makedirs(log_dir, exist_ok=True)

# Filename: Reconciliation_YYYY-MM-DD.log
log_filename = os.path.join(log_dir, f"Reconciliation_{today}.log")

# Setup logger
logger = logging.getLogger("DailyLogger")
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Avoid adding duplicate handlers
file_handler = logging.FileHandler(log_filename, mode='a')
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


logger.info("Logger initialized successfully.")