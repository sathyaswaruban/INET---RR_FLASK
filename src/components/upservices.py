import pandas as pd
from db_connector import get_db_connection
from logger_config import logger
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Any, Optional
from sqlalchemy import text
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from sqlalchemy.exc import OperationalError, DatabaseError

engine = get_db_connection()

# Configure retry logic for database operations
DB_RETRY_CONFIG = {
    "stop": stop_after_attempt(3),
    "wait": wait_exponential(multiplier=1, min=1, max=10),
    "retry": retry_if_exception_type((OperationalError, DatabaseError)),
    "reraise": True,
}


@retry(**DB_RETRY_CONFIG)
def execute_sql_with_retry(query, params=None):
    logger.info("Entered helper function to execute SQL with retry logic")
    with engine.connect().execution_options(stream_results=True) as connection:
        try:
            df = pd.read_sql(query, con=connection, params=params)
            return df
        except Exception as e:
            logger.error(f"Error during SQL execution: {e}")
            raise  # This will trigger retry if it's an OperationalError or DatabaseError

    # Add other service configurations here


def up_service_selection(
    start_date: str, end_date: str, service_name: str, df_excel: pd.DataFrame
) -> Any:
    """Handle up service selection and processing."""
    logger.info(f"Entering Reconciliation for {service_name} Service")

    # Validate service name
    if service_name not in SERVICE_CONFIGS:
        logger.warning("OutwardService function selection Error")
        return "Service Name Error..!"

    service_config = SERVICE_CONFIGS[service_name]

    # Validate required columns
    if not all(col in df_excel.columns for col in service_config["required_columns"]):
        logger.warning(f"Wrong File Uploaded in {service_name} Service")
        return "Wrong File Uploaded...!"

    try:

        hub_data = service_config["service_func"](start_date, end_date, service_name)
        return filtering_Data(hub_data, df_excel, service_name)

    except Exception as e:
        logger.error(f"Error processing {service_name} service: {str(e)}")
        return f"Error processing {service_name} service"


def filtering_Data(df_db, df_excel, service_name):
    try:
        logger.info(f"Filteration Starts for {service_name} service")
        mapping = None
        message = None
        Excel_count = len(df_excel)
        Hub_count = None
        df_db["SERVICE_DATE"] = pd.to_datetime(
            df_db["SERVICE_DATE"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        df_excel["VENDOR_DATE"] = pd.to_datetime(
            df_excel["VENDOR_DATE"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        # tenant_data["TENANT_STATUS"] = tenant_data["TENANT_STATUS"].apply(
        #     lambda x: status_mapping.get(x, x)
        # )

        # function to select only required cols and make it as df
        def safe_column_select(df, columns):
            existing_cols = [col for col in columns if col in df.columns]
            return df[existing_cols].copy()

        # Required columns that to be sent as result to UI
        required_columns = [
            "EBO_ID",
            "USERNAME",
            "VLE_ID",
            "REFID",
            "SERVICE_CODE",
            "APPLICATION_COUNT",
            "RATE",
            "TOTAL_AMOUNT",
            "JENSEVA_TYPE",
            "VENDOR_DATE",
            "SERVICE_DATE",
            "SUB_DISTRICT",
            "VILLAGE",
        ]

        matched = df_db.merge(
            df_excel, left_on="VENDOR_REFERENCE", right_on="REFID", how="inner"
        ).copy()
        matched["CATEGORY"] = "MATCHED"
        matched = safe_column_select(matched, required_columns)

        # 1 Filtering Data initiated in IHUB portal and not in Vendor Xl
        not_in_vendor = df_db[~df_db["VENDOR_REFERENCE"].isin(df_excel["REFID"])].copy()
        not_in_vendor["CATEGORY"] = "NOT_IN_VENDOR"
        not_in_vendor= not_in_vendor.rename(columns={"VENDOR_REFERENCE" : "REFID"})
        not_in_vendor = safe_column_select(not_in_vendor, required_columns)
        # 2. Filtering Data Present in Vendor XL but Not in Ihub Portal
        not_in_portal_1 = df_excel[
            ~df_excel["REFID"].isin(df_db["VENDOR_REFERENCE"])
        ].copy()
        not_in_portal_app_ids = not_in_portal_1["REFID"]

        if not not_in_portal_app_ids.dropna().empty :
            query=text("""
            SELECT u.UserName as EBO_ID, ed.Name as USERNAME, udt.VleId as VLE_ID, 
                   udt.ApplicationId as VENDOR_REFERENCE, uds.Code as SERVICE_CODE, 
                   udt.Count as APPLICATION_COUNT, udt.Commission as RATE, 
                   udt.Amount as TOTAL_AMOUNT, 
                   CASE WHEN u.jansevaType = 1 THEN 'Jenseva Kendra'
                        WHEN u.jansevaType = 2 THEN 'Panchayat Sahayak'
                        ELSE 'EBO' END as JENSEVA_TYPE,
                   udt.CreationTs AS SERVICE_DATE, sd.Name AS SUB_DISTRICT, 
                   ed.VillageName As VILLAGE
            FROM tenantinetcsc.UpeDistrictTransaction udt
            LEFT JOIN tenantinetcsc.EboDetail ed ON ed.id = udt.EboDetailId
            LEFT JOIN tenantinetcsc.`User` u ON ed.UserId = u.id
            LEFT JOIN tenantinetcsc.UpeDistrictService uds ON uds.id = udt.UpeDistrictServiceId
            LEFT JOIN tenantinetcsc.SubDistrict sd ON sd.id = ed.SubDistrictId
            WHERE udt.ApplicationId IN :app_ids
            """)
            params = {"app_ids": tuple(not_in_portal_app_ids)}
            not_in_portal_1_db_check = execute_sql_with_retry(query, params=params)
            in_portal_date_diff_df = not_in_portal_1_db_check.merge(
            df_excel,left_on="VENDOR_REFERENCE", right_on="REFID", how="inner"
            ).copy()
            in_portal_date_diff_df["CATEGORY"] = 'IN_PORTAL_DIFF_DATE'
            not_in_portal = not_in_portal_1[~not_in_portal_1['REFID'].isin(in_portal_date_diff_df["VENDOR_REFERENCE"])].copy()
            in_portal_date_diff_df["SERVICE_DATE"] = pd.to_datetime(
                in_portal_date_diff_df["SERVICE_DATE"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
            in_portal_date_diff_df = safe_column_select(in_portal_date_diff_df, required_columns)

            not_in_portal["CATEGORY"] = "NOT_IN_PORTAL"
            not_in_portal = safe_column_select(not_in_portal, required_columns)
            success_count = matched.shape[0] + in_portal_date_diff_df.shape[0]
            Hub_count = df_db.shape[0] + in_portal_date_diff_df.shape[0]
        else :
            if not_in_portal_1.empty and not_in_vendor.empty:
                message= "Hurray..! There is no mismatch Data Found."
                
            not_in_portal= not_in_portal_1
            Hub_count = df_db.shape[0]
            success_count = matched.shape[0]
            in_portal_date_diff_df = None

        # 4. Filtering Data that matches in both Ihub Portal and Vendor Xl as : Matched
        # 5. Filtering Data that Mismatched in both Ihub Portal and Vendor Xl as : Mismatched
        failed_count = not_in_portal.shape[0] + not_in_vendor.shape[0]

        logger.info("Filteration Ends")
        mapping = {
            "not_in_vendor": not_in_vendor,
            "not_in_Portal": not_in_portal,
            "In_portal_date_diff": in_portal_date_diff_df,
            "matched": matched,
            "Excel_value_count": Excel_count,
            "HUB_Value_count": Hub_count,
            "Total_Success_count": success_count,
            "Total_Failed_count": failed_count,
            "message":message
        }
        return mapping

    except Exception as e:
        logger.warning("Error inside Filtering Function: %s", e)
        print(f"Error inside Filtering Function: {e}")
        message = "Error in Filteration"
        return message


# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# UpiQr_Service Function
def sultanpu_sca_service_function(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        """ SELECT u.UserName as EBO_ID,ed.Name as USERNAME,udt.VleId as VLE_ID,udt.ApplicationId as VENDOR_REFERENCE ,uds.Code as SERVICE_CODE,udt.Count as APPLICATION_COUNT,udt.Commission as RATE,udt.Amount as TOTAL_AMOUNT,
            CASE WHEN u.jansevaType = 1 THEN 'Jenseva Kendra'
                WHEN u.jansevaType = 2 THEN 'Panchayat Sahayak'
                ELSE 'EBO' 
                END as JENSEVA_TYPE,
                udt.CreationTs AS SERVICE_DATE,sd.Name AS SUB_DISTRICT,ed.VillageName As VILLAGE
            FROM tenantinetcsc.UpeDistrictTransaction udt 
            LEFT JOIN tenantinetcsc.EboDetail ed on ed.id = udt.EboDetailId 
            LEFT JOIN tenantinetcsc.`User` u on ed.UserId = u.id
            LEFT JOIN tenantinetcsc.UpeDistrictService uds on uds.id = udt.UpeDistrictServiceId
            LEFT JOIN tenantinetcsc.SubDistrict sd on sd.id = ed.SubDistrictId
        where DATE(udt.CreationTs) BETWEEN :start_date AND :end_date and udt.UpeDistrictServiceId not in (1)
    """
    )
    params = {
        "start_date": start_date,
        "end_date": end_date,
    }
    try:
        # Safe query execution with retry
        df_db = execute_sql_with_retry(query, params=params)

        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()

        df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
        result = df_db

    except SQLAlchemyError as e:
        logger.error(f"Database error in UPIQR_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in UPIQR_Service(): {e}")

    return result

def sultanpu_integrated_service_function(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        """ SELECT u.UserName as EBO_ID,ed.Name as USERNAME,udt.VleId as VLE_ID,udt.QuotaId as VENDOR_REFERENCE ,uds.Code as SERVICE_CODE,udt.Count as APPLICATION_COUNT,udt.Commission as RATE,udt.Amount as TOTAL_AMOUNT,
            CASE WHEN u.jansevaType = 1 THEN 'Jenseva Kendra'
                WHEN u.jansevaType = 2 THEN 'Panchayat Sahayak'
                ELSE 'EBO' 
                END as JENSEVA_TYPE,
                udt.CreationTs AS SERVICE_DATE,sd.Name AS SUB_DISTRICT,ed.VillageName As VILLAGE
            FROM tenantinetcsc.UpeDistrictTransaction udt 
            LEFT JOIN tenantinetcsc.EboDetail ed on ed.id = udt.EboDetailId 
            LEFT JOIN tenantinetcsc.`User` u on ed.UserId = u.id
            LEFT JOIN tenantinetcsc.UpeDistrictService uds on uds.id = udt.UpeDistrictServiceId
            LEFT JOIN tenantinetcsc.SubDistrict sd on sd.id = ed.SubDistrictId
        where DATE(udt.CreationTs) BETWEEN :start_date AND :end_date and udt.UpeDistrictServiceId in (1)
    """
    )
    params = {
        "start_date": start_date,
        "end_date": end_date,
    }
    try:
        # Safe query execution with retry
        df_db = execute_sql_with_retry(query, params=params)

        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()

        df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
        result = df_db

    except SQLAlchemyError as e:
        logger.error(f"Database error in UPIQR_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in UPIQR_Service(): {e}")

    return result


SERVICE_CONFIGS = {
    "SULTANPURSCA": {
        "required_columns": ["REFID"],
        "service_func": sultanpu_sca_service_function,
    },
    "SULTANPUR_IS": {
        "required_columns": ["REFID"],
        "service_func": sultanpu_integrated_service_function,
    },
}
