import os
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime

# Ensure 'log' directory exists
os.makedirs("logs", exist_ok=True)

# Define log file path inside 'log' folder
log_filename = os.path.join("logs", f"Reconciliation_{datetime.date.today()}.log")

# Create a logger
logger = logging.getLogger("DailyLogger")
logger.setLevel(logging.INFO)

# Create a TimedRotatingFileHandler (New file every day at midnight)
handler = TimedRotatingFileHandler(
    log_filename, when="midnight", interval=1, backupCount=7
)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

# Add handler to logger
logger.addHandler(handler)
