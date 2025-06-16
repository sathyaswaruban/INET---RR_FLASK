from outwardservice import outward_service_selection
import pandas as pd
from logger_config import logger
from inwardservice import inward_service_selection
from handler import handler


def main(from_date, to_date, service_name, file, transaction_type):
    try:
        logger.info("--------------------------------------------")
        logger.info("Entered Main Function...")
        df_excel = pd.read_excel(file, dtype=str)
        if service_name == "Pan_UTI":
            df_excel = df_excel.rename(
                columns={"Refrence No": "REFID", "trans Date": "DATE"}
            )

        df_excel["DATE"] = pd.to_datetime(df_excel["DATE"], errors="coerce").dt.date

        from_date = pd.to_datetime(from_date).date()
        to_date = pd.to_datetime(to_date).date()

        Date_check = df_excel[
            (df_excel["DATE"] >= from_date) & (df_excel["DATE"] <= to_date)
        ]

        if Date_check.empty:
            logger.warning("No records found within the given date range..!")
            message = "No records found within the given date range..!"
            return message

        logger.info("Records found within the date range. Running reconciliation...")
        print(service_name)
        if service_name in ["AEPS", "MATM", "UPIQR"]:
            result = inward_service_selection(
                from_date, to_date, service_name, transaction_type, df_excel
            )
        elif service_name in [
            "RECHARGE",
            "IMT",
            "Pan_NSDL",
            "Pan_UTI",
            "BBPS",
            "PASSPORT",
        ]:
            result = outward_service_selection(
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

    except Exception as e:
        logger.error("Error in main(): %s", str(e))
        return {"status": "500", "error": str(e)}
