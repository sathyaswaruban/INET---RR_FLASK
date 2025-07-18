from filteration_process import outward_service_selection
import pandas as pd
from logger_config import logger
from inwardservice import inward_service_selection
from handler import handler
from upiQrfiltering import upiQr_service_selection

# Define service configurations as constants
SERVICE_CONFIGS = {
    "BBPS": {
        "columns": {
            "Transaction Date": "VENDOR_DATE",
            "Transaction Ref ID": "REFID",
            "NPCI Transaction Desc": "VENDOR_STATUS",
        },
        "date_format": "%d-%b-%Y %H:%M:%S",
    },
    "PASSPORT": {
        "columns": {
            "Ref No": "REFID",
            "Txn Date": "VENDOR_DATE",
            "Status": "VENDOR_STATUS",
        }
    },
    "AEPS": {
        "columns": {
            "SERIALNUMBER": "REFID",
            "DATE": "VENDOR_DATE",
            "STATUS": "VENDOR_STATUS",
        }
    },
    "RECHARGE": {"columns": {"DATE": "VENDOR_DATE", "STATUS": "VENDOR_STATUS"}},
    "PANUTI": {
        "columns": {
            "Refrence No": "REFID",
            "trans Date": "VENDOR_DATE",
            "Payment Status": "VENDOR_STATUS",
        }
    },
    "PANNSDL": {
        "columns": {
            "Acknowledgment Number": "REFID",
            "Date": "VENDOR_DATE",
            "Status Of Application": "VENDOR_STATUS",
        }
    },
    "MATM": {
        "columns": {
            "Date": "VENDOR_DATE",
            "Remarks": "VENDOR_STATUS",
            "Utr": "REFID",
            "Amount": "AMOUNT",
        }
    },
    "LIC": {
        "columns": {
            "ORDERID": "REFID",
            "Date": "VENDOR_DATE",
            "STATUS": "VENDOR_STATUS",
        }
    },
    "ASTRO": {
        "columns": {
            "Order ID": "REFID",
            "Date": "VENDOR_DATE",
            "Transaction Status": "VENDOR_STATUS",
            "Price": "AMOUNT",
        }
    },
    "UPIQR": {
        "columns": {
            "TRNSCTN_NMBR": "REFID",
            "ATHRSD_DATE": "VENDOR_DATE",
            "Status": "VENDOR_STATUS",
        },
        "day_first": True,
    },
    "DMT": {
        "columns": {
            "DATE": "VENDOR_DATE",
            "STATUS": "VENDOR_STATUS",
        }
    },
}

INWARD_SERVICES = {"AEPS", "MATM"}
OUTWARD_SERVICES = {
    "RECHARGE",
    "IMT",
    "Pan_NSDL",
    "LIC",
    "BBPS",
    "PASSPORT",
    "ABHIBUS",
    "ASTRO",
    "PANUTI",
    "PANNSDL",
    "DMT",
}


def process_date_columns(df, service_name, service_config):
    if "VENDOR_DATE" in df:
        date_params = {
            "errors": "coerce",
            "dayfirst": service_config.get("day_first", False),
        }

        if "date_format" in service_config:
            date_params["format"] = service_config["date_format"]

        df["VENDOR_DATE"] = pd.to_datetime(df["VENDOR_DATE"], **date_params).dt.date
    return df


def select_service_handler(
    service_name, from_date, to_date, df_excel, transaction_type=None
):
    """Select the appropriate service handler based on service type"""
    if service_name in INWARD_SERVICES:
        logger.info(f"inward_service: {service_name}")
        return inward_service_selection(
            from_date, to_date, service_name, transaction_type, df_excel
        )
    elif service_name in OUTWARD_SERVICES:
        logger.info(f"outward_service: {service_name}")
        return outward_service_selection(from_date, to_date, service_name, df_excel)
    elif service_name == "UPIQR":
        logger.info(f"UpiQr_service: {service_name}")
        return upiQr_service_selection(from_date, to_date, service_name, df_excel)
    return None


def main(from_date, to_date, service_name, file, transaction_type=None):
    try:
        logger.info("--------------------------------------------")
        logger.info("Entered Main Function...")

        # Validate service name
        if service_name not in SERVICE_CONFIGS:
            logger.warning("Error in Service name..!")
            return "Error in Service name..!"

        # Read and process the Excel file
        df_excel = pd.read_excel(file, dtype=str)
        service_config = SERVICE_CONFIGS[service_name]

        # Rename columns based on service configuration
        df_excel = df_excel.rename(columns=service_config["columns"])

        # Process date columns
        df_excel = process_date_columns(df_excel, service_name, service_config)

        # Convert input dates
        from_date = pd.to_datetime(from_date).date()
        to_date = pd.to_datetime(to_date).date()

        # Filter by date range
        date_mask = (df_excel["VENDOR_DATE"] >= from_date) & (
            df_excel["VENDOR_DATE"] <= to_date
        )
        date_check = df_excel[date_mask]

        if date_check.empty:
            logger.warning("No records found within the given date range..!")
            return "No records found within the given date range..!"

        logger.info("Records found within the date range. Running reconciliation...")

        # Select and execute the appropriate service handler
        result = select_service_handler(
            service_name, from_date, to_date, df_excel, transaction_type
        )

        if result is None:
            logger.warning("Error in selecting service handler in Main.py!")
            return "Error processing file..!"

        logger.info("Reconciliation Ends")
        return result

    except Exception as e:
        logger.error("Error in main(): %s", str(e))
        return "Something Went wrong..!"
