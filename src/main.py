from components.filteration_process import service_selection
import pandas as pd
from logger_config import logger
from components.upiQrfiltering import upiQr_service_selection
from components.upservices import up_service_selection
from components.iti_imps import imps_service_function
# Define service configurations as constants
SERVICE_CONFIGS = {
    "BBPS": {
        "columns": {
            "Transaction Date": "VENDOR_DATE",
            "Transaction Ref ID": "REFID",
            "NPCI Transaction Desc": "VENDOR_STATUS",
        },
        "required_columns": ["Transaction Ref ID"],
        "date_format": "%d-%b-%Y %H:%M:%S",
    },
    "PASSPORT": {
        "columns": {
            "Ref No": "REFID",
            "Txn Date": "VENDOR_DATE",
            "Status": "VENDOR_STATUS",
            "        Debit": "AMOUNT",
        },
        "required_columns": ["Ref No"],
    },
    "AEPS": {
        "columns": {
            "SERIALNUMBER": "REFID",
            "DATE": "VENDOR_DATE",
            "STATUS": "VENDOR_STATUS",
        },
        "columnsmini": {
            "SERIALNUMBER": "REFID",
            "ADDEDDATE": "VENDOR_DATE",
            "STATUS": "VENDOR_STATUS",
        },
        "required_columns": ["SERIALNUMBER"],
    },
    "RECHARGE": {
        "columns": {"DATE": "VENDOR_DATE", "STATUS": "VENDOR_STATUS"},
        "required_columns": ["REFID"],
    },
    "PANUTI": {
        "columns": {
            "Refrence No": "REFID",
            "trans Date": "VENDOR_DATE",
            "Payment Status": "VENDOR_STATUS",
        },
        "required_columns": ["Refrence No"],
    },
    "PANNSDL": {
        "columns": {
            "Acknowledgment Number": "REFID",
            "Date": "VENDOR_DATE",
            "Status Of Application": "VENDOR_STATUS",
        },
        "required_columns": ["Acknowledgment Number"],
    },
    "MATM": {
        "columns": {
            "Date": "VENDOR_DATE",
            "TRANSACTIONSTATUS": "VENDOR_STATUS",
            "RRN": "REFID",
        },
        "required_columns": ["RRN"],
    },
    "LIC": {
        "columns": {
            "ORDERID": "REFID",
            "Date": "VENDOR_DATE",
            "STATUS": "VENDOR_STATUS",
        },
        "required_columns": ["ORDERID"],
    },
    "ASTRO": {
        "columns": {
            "Order ID": "REFID",
            "Date": "VENDOR_DATE",
            "Transaction Status": "VENDOR_STATUS",
            "Price": "AMOUNT",
        },
        "required_columns": ["Order ID"],
    },
    "UPIQR": {
        "columns": {
            "Unique_ID": "REFID",
            "ATHRSD_DATE": "VENDOR_DATE",
            "STATUS": "VENDOR_STATUS",
        },
        "day_first": True,
        "required_columns": ["Unique_ID"],
    },
    "DMT": {
        "columns": {
            "DATE": "VENDOR_DATE",
            "STATUS": "VENDOR_STATUS",
        },
        "required_columns": ["REFID"],
    },
    "INSURANCE_OFFLINE": {
        "columns": {
            "Issued Date": "VENDOR_DATE",
            "Status": "VENDOR_STATUS",
            "Policy No": "REFID",
        },
        "required_columns": ["Policy No"],
    },
    "ABHIBUS": {
        "columns": {
            "Tkt. Number": "REFID",
            "Booked Date": "VENDOR_DATE",
            "Status": "VENDOR_STATUS",
        },
        "required_columns": ["Tkt. Number"],
    },
    "SULTANPURSCA": {
        "columns": {
            "Application ID": "REFID",
            "Transaction Date": "VENDOR_DATE",
        },
        "date_format": "%d/%m/%Y",
        "day_first": True,
        "required_columns": ["Application ID"],
    },
    "CHITRAKOOT_SCA": {
        "columns": {
            "Application ID": "REFID",
            "Transaction Date": "VENDOR_DATE",
        },
        "date_format": "%d/%m/%Y",
        "day_first": True,
        "required_columns": ["Application ID"],
    },
    "SULTANPUR_IS": {
        "columns": {
            "Quota ID": "REFID",
            "Transaction Date": "VENDOR_DATE",
        },
        "required_columns": ["Quota ID"],
        # "date_format": "%d-%m-%Y %H:%M:%S",
    },
    "CHITRAKOOT_IS": {
        "columns": {
            "Quota ID": "REFID",
            "Transaction Date": "VENDOR_DATE",
        },
        "required_columns": ["Quota ID"],
    },
    "MOVETOBANK": {
        "columns": {
            "Corporate Ref No": "REFID",
            "Transaction Status": "VENDOR_STATUS",
            "Creation Date": "VENDOR_DATE",
        },
        "required_columns": ["Corporate Ref No"],
        # "date_format": "%d-%m-%Y %H:%M:%S",
    },
    "MANUAL_TB": {
        "columns": {
            "Description": "REFID",
            "Status": "VENDOR_STATUS",
            "Txn / Value Date": "VENDOR_DATE",
        },
        "required_columns": ["Description"],
        # "date_format": "%d-%m-%Y %H:%M:%S", service function in upservice module
    },
    "IMPS": {
        "columns": {
            "UTR Number": "REFID",
            "Status": "VENDOR_STATUS",
            "Transaction Date": "VENDOR_DATE",
            "Amount": "VENDOR_AMOUNT",
        },
        "required_columns": ["UTR Number"],
        # "date_format": "%d-%m-%Y %H:%M:%S",
    },
}

UPPS_SERVICES = {
    "SULTANPURSCA",
    "SULTANPUR_IS",
    "CHITRAKOOT_IS",
    "CHITRAKOOT_SCA",
    "MANUAL_TB",
}


def process_date_columns(df, service_name, service_config):
    try:
        if "VENDOR_DATE" in df:
            date_params = {
                "errors": "coerce",
                "dayfirst": service_config.get("day_first", False),
            }

            if "date_format" in service_config:
                date_params["format"] = service_config["date_format"]

            df["VENDOR_DATE"] = pd.to_datetime(df["VENDOR_DATE"], **date_params).dt.date
    except Exception as e:
        print(e)
        logger.error("Error in Date_Processing(): %s", str(e))
    return df


def select_service_handler(
    service_name, from_date, to_date, df_excel, transaction_type=None
):
    """Select the appropriate service handler based on service type"""
    if service_name == "UPIQR":
        logger.info(f"UpiQr_service: {service_name}")
        return upiQr_service_selection(from_date, to_date, service_name, df_excel)
    elif service_name in UPPS_SERVICES:
        logger.info(f"Upps_service: {service_name}")
        return up_service_selection(from_date, to_date, service_name, df_excel)
    elif service_name == "IMPS":
        logger.info(f"Ihub service: {service_name}")
        return imps_service_function(from_date, to_date, service_name,df_excel)
    else:
        logger.info(f"Ihub service: {service_name}")
        return service_selection(
            from_date, to_date, service_name, df_excel, transaction_type
        )


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
        if not all(
            col in df_excel.columns for col in service_config["required_columns"]
        ):
            logger.warning(f"Wrong File Uploaded in {service_name} Service")
            return "Wrong File Uploaded...!"

        if transaction_type == "3":
            df_excel = df_excel.rename(columns=service_config["columnsmini"])
        else:
            # Rename columns based on service configuration
            df_excel = df_excel.rename(columns=service_config["columns"])

        if service_name in ["INSURANCE_OFFLINE", "SULTANPUR_IS", "CHITRAKOOT_IS"]:
            if service_name == "INSURANCE_OFFLINE":
                df_excel["REFID"] = df_excel["REFID"].astype(str).str.slice(0, 20)
            else:
                df_excel["REFID"] = df_excel["REFID"].astype(str).str.slice(1, 11)
        # print(df_excel["REFID"].head(5))
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
