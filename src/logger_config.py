import os
import logging
from datetime import datetime, time, timedelta


class DailyRenameFileHandler(logging.FileHandler):
    """
    Logs into logs/YYYY/MM/Reconciliation.log during the day.
    At ~23:50, renames Reconciliation.log to Reconciliation-YYYY-MM-DD.log
    in the same YYYY/MM folder.
    Starts a fresh Reconciliation.log for the next day.
    Deletes logs older than retention_days.
    """

    def __init__(
        self,
        base_dir,
        filename="Reconciliation.log",
        encoding="utf-8",
        retention_days=90,
    ):
        self.base_dir = base_dir
        self.filename = filename
        self.last_rollover_date = None
        self.retention_days = retention_days

        # Ensure today's directory and base file path
        self.base_filename = self._get_current_log_path()
        os.makedirs(os.path.dirname(self.base_filename), exist_ok=True)

        super().__init__(self.base_filename, encoding=encoding)

    def _get_current_log_path(self):
        """Return current day's active log path: logs/YYYY/MM/Reconciliation.log"""
        now = datetime.now()
        dated_dir = os.path.join(self.base_dir, str(now.year), f"{now.month:02d}")
        return os.path.join(dated_dir, self.filename)

    def emit(self, record):
        """Write logs and check if rollover needed at ~23:50."""
        now = datetime.now()

        # Check if it's time to rollover
        if now.time() >= time(23, 50) and (self.last_rollover_date != now.date()):
            self.doRollover(now)

        super().emit(record)

    def doRollover(self, now):
        """Rename current log file with today's date inside YYYY/MM folder."""
        self.stream.close()

        dated_dir = os.path.join(self.base_dir, str(now.year), f"{now.month:02d}")
        os.makedirs(dated_dir, exist_ok=True)

        # Rename current day's log
        dated_filename = os.path.join(
            dated_dir, f"Reconciliation-{now.strftime('%Y-%m-%d')}.log"
        )

        if os.path.exists(self.base_filename):
            os.rename(self.base_filename, dated_filename)

        # Update base_filename for next day
        self.base_filename = self._get_current_log_path()

        # Open new Reconciliation.log for next day
        self.stream = self._open()
        self.last_rollover_date = now.date()

        # Cleanup old logs
        self.cleanup_old_logs()

    def cleanup_old_logs(self):
        """Delete logs older than retention_days across YYYY/MM folders."""
        cutoff_date = datetime.now().date() - timedelta(days=self.retention_days)

        for root, _, files in os.walk(self.base_dir):
            for f in files:
                if f.startswith("Reconciliation-") and f.endswith(".log"):
                    try:
                        date_str = f.replace("Reconciliation-", "").replace(".log", "")
                        log_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                        if log_date < cutoff_date:
                            os.remove(os.path.join(root, f))
                    except Exception:
                        pass  # Ignore unexpected names


base_log_dir = "D:/INET_RR_FLASK/logs"
logger = logging.getLogger("DailyLogger")
logger.setLevel(logging.INFO)

# Remove old handlers
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

file_handler = DailyRenameFileHandler(base_dir=base_log_dir, retention_days=90)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.info("Logger initialized Successfully.")
