from outwardservice import outward_service_selection
import pandas as pd
from logger_config import logger
from inwardservice import inward_service_selection
from handler import handler
from upiQrfiltering import upiQr_service_selection


def main(from_date, to_date, service_name, file, transaction_type):
    try:

        logger.info("--------------------------------------------")
        logger.info("Entered Main Function...")

        df_excel = pd.read_excel(file, dtype=str)

        if service_name == "BBPS":
            df_excel = df_excel.rename(
                columns={
                    "Transaction Date": "VENDOR_DATE",
                    "Transaction Ref ID": "REFID",
                    "NPCI Transaction Desc": "VENDOR_STATUS",
                }
            )
        elif service_name == "PASSPORT":

            df_excel = df_excel.rename(
                columns={
                    "Ref No": "REFID",
                    "Txn Date": "VENDOR_DATE",
                    "Status": "VENDOR_STATUS",
                }
            )
        elif service_name == "AEPS":
            df_excel = df_excel.rename(
                columns={
                    "SERIALNUMBER": "REFID",
                    "DATE": "VENDOR_DATE",
                    "STATUS": "VENDOR_STATUS",
                }
            )
        elif service_name == "RECHARGE":
            df_excel = df_excel.rename(
                columns={"DATE": "VENDOR_DATE", "STATUS": "VENDOR_STATUS"}
            )
        elif service_name == "PANUTI":
            df_excel = df_excel.rename(
                columns={
                    "Refrence No": "REFID",
                    "trans Date": "VENDOR_DATE",
                    "Payment Status": "VENDOR_STATUS",
                }
            )
        elif service_name == "MATM":
            df_excel = df_excel.rename(
                columns={
                    "Date": "VENDOR_DATE",
                    "Remarks": "VENDOR_STATUS",
                    "Utr": "REFID",
                    "Amount": "AMOUNT",
                }
            )
        elif service_name == "LIC":
            df_excel = df_excel.rename(
                columns={
                    "ORDERID": "REFID",
                    "Date": "VENDOR_DATE",
                    "STATUS": "VENDOR_STATUS",
                }
            )
        elif service_name == "ASTRO":
            df_excel = df_excel.rename(
                columns={
                    "Order ID": "REFID",
                    "Date": "VENDOR_DATE",
                    "Transaction Status": "VENDOR_STATUS",
                }
            )
        elif service_name == "UPIQR":
            df_excel = df_excel.rename(
                columns={
                    "TRNSCTN_NMBR": "REFID",
                    "ATHRSD_DATE": "VENDOR_DATE",
                    "Status": "VENDOR_STATUS",
                    # "Settled_Amount": "AMOUNT",
                }
            )
        else:
            message = "Error in Service name..!"
            return message

        if "VENDOR_DATE" in df_excel:
            if service_name == "UPIQR":

                df_excel["VENDOR_DATE"] = pd.to_datetime(
                    df_excel["VENDOR_DATE"], errors="coerce", dayfirst=True
                ).dt.date
            elif service_name == "BBPS":
                df_excel["VENDOR_DATE"] = pd.to_datetime(
                    df_excel["VENDOR_DATE"], format="%d-%b-%Y %H:%M:%S", errors="coerce"
                ).dt.date
            else:
                df_excel["VENDOR_DATE"] = pd.to_datetime(
                    df_excel["VENDOR_DATE"], errors="coerce"
                ).dt.date
                print(df_excel["VENDOR_DATE"])
            from_date = pd.to_datetime(from_date).date()
            to_date = pd.to_datetime(to_date).date()

            Date_check = df_excel[
                (df_excel["VENDOR_DATE"] >= from_date)
                & (df_excel["VENDOR_DATE"] <= to_date)
            ]

            if Date_check.empty:
                logger.warning("No records found within the given date range..!")
                message = "No records found within the given date range..!"
                return message

            logger.info(
                "Records found within the date range. Running reconciliation..."
            )
            if service_name in ["AEPS", "MATM"]:
                print("inward_service:", service_name)
                result = inward_service_selection(
                    from_date, to_date, service_name, transaction_type, df_excel
                )
            elif service_name in [
                "RECHARGE",
                "IMT",
                "Pan_NSDL",
                "LIC",
                "BBPS",
                "PASSPORT",
                "ABHIBUS",
                "ASTRO",
                "PANUTI",
            ]:
                print("outward_service:", service_name)
                result = outward_service_selection(
                    from_date, to_date, service_name, df_excel
                )
            elif service_name == "UPIQR":
                print("UpiQr_service:", service_name)
                result = upiQr_service_selection(
                    from_date, to_date, service_name, df_excel
                )
            else:
                logger.warning(
                    "Error in selecting outward or inwardward service function in Main.py!"
                )
                message = "Error processing file..!"
                return message

            logger.info("Reconciliation Ends")
            return result
        else:
            message = "Wrong File Uploaded..!"
            return message

    except Exception as e:
        logger.error("Error in main(): %s", str(e))
        message = "Something Went wrong..!"
        return message
