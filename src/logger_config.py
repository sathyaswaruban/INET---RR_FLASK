import os
import logging
from datetime import datetime, timedelta


class DailyRenameFileHandler(logging.FileHandler):
    """
    Logs into logs/YYYY/MM/Reconciliation.log during the day.
    At rollover (next log on a new day), renames Reconciliation.log
    to Reconciliation-YYYY-MM-DD.log based on the file's last write date.
    Starts a fresh Reconciliation.log for the current day.
    Deletes logs older than retention_days.
    Handles restarts safely (keeps appending to today's log if it already exists).
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
        """Write logs and check if rollover needed on date change."""
        now = datetime.now()

        # If first log of the day → rollover
        if self.last_rollover_date is None or self.last_rollover_date != now.date():
            self.doRollover(now)

        super().emit(record)

    def doRollover(self, now):
        """Rename current log file with correct last write date and start fresh log."""
        if self.stream:
            self.stream.close()

        rollover_date = None

        # Check if existing log belongs to today (restart safe)
        if os.path.exists(self.base_filename):
            mtime = datetime.fromtimestamp(os.path.getmtime(self.base_filename))
            file_date = mtime.date()

            if file_date == now.date():
                # It's already today's log → no rollover
                self.stream = self._open()
                self.last_rollover_date = now.date()
                return
            else:
                rollover_date = file_date
        else:
            rollover_date = now.date()

        # Save into YYYY/MM folder
        dated_dir = os.path.join(self.base_dir, str(rollover_date.year), f"{rollover_date.month:02d}")
        os.makedirs(dated_dir, exist_ok=True)

        dated_filename = os.path.join(
            dated_dir, f"Reconciliation-{rollover_date.strftime('%Y-%m-%d')}.log"
        )

        # Rename existing log if it exists
        if os.path.exists(self.base_filename):
            os.rename(self.base_filename, dated_filename)

        # Start fresh Reconciliation.log for today
        self.base_filename = self._get_current_log_path()
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


# ===============================
# LOGGER SETUP
# ===============================

base_log_dir = "D:/INET_RR_FLASK/logs"

logger = logging.getLogger("DailyLogger")
logger.setLevel(logging.INFO)

# Remove old handlers (avoid duplicates on reload)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Add custom file handler
file_handler = DailyRenameFileHandler(base_dir=base_log_dir, retention_days=90)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.info("Logger initialized Successfully.")
