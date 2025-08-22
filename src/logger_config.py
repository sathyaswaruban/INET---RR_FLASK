import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime


class DatedPathTimedRotatingFileHandler(TimedRotatingFileHandler):
    """
    Custom TimedRotatingFileHandler that creates logs in
    logs/YYYY/MM/Reconciliation-YYYY-MM-DD.log
    """

    def __init__(
        self,
        base_dir,
        filename,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    ):
        self.base_dir = base_dir
        self.base_filename = filename  # e.g., "Reconciliation.log"
        os.makedirs(base_dir, exist_ok=True)

        # Build initial file path inside YYYY/MM
        dated_dir = self._get_dated_dir()
        os.makedirs(dated_dir, exist_ok=True)
        full_path = os.path.join(dated_dir, self.base_filename)

        super().__init__(
            full_path,
            when=when,
            interval=interval,
            backupCount=backupCount,
            encoding=encoding,
        )

    def _get_dated_dir(self):
        """Return directory path logs/YYYY/MM/"""
        now = datetime.now()
        return os.path.join(self.base_dir, str(now.year), f"{now.month:02d}")

    def doRollover(self):
        """
        Override rollover: put rotated files inside YYYY/MM with date suffix.
        """
        self.stream.close()

        # Ensure year/month dir exists for new log
        dated_dir = self._get_dated_dir()
        os.makedirs(dated_dir, exist_ok=True)

        # New log filename (with date suffix)
        dfn = os.path.join(
            dated_dir, f"Reconciliation-{datetime.now().strftime('%Y-%m-%d')}.log"
        )

        # Rename current log to dated file
        if os.path.exists(self.baseFilename):
            os.rename(self.baseFilename, dfn)

        # Open new stream for today's log
        self.stream = self._open()

        # Cleanup old files if exceeding backupCount
        if self.backupCount > 0:
            # List all rotated files in all year/month folders
            all_logs = []
            for root, _, files in os.walk(self.base_dir):
                for f in files:
                    if f.startswith("Reconciliation-") and f.endswith(".log"):
                        all_logs.append(os.path.join(root, f))
            all_logs.sort(reverse=True)
            for old_file in all_logs[self.backupCount :]:
                os.remove(old_file)



base_log_dir = "D:/INET_RR_FLASK/logs"
logger = logging.getLogger("DailyLogger")
logger.setLevel(logging.INFO)

# Remove old handlers
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Attach custom handler
file_handler = DatedPathTimedRotatingFileHandler(
    base_dir=base_log_dir,
    filename="Reconciliation.log",
    when="midnight",
    interval=1,
    backupCount=30,
)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Test log
logger.info("Logger initialized with custom year/month rotation.")
