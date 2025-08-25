import pandas as pd
from logger_config import logger
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from db_connector import get_db_connection
from logger_config import logger
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from sqlalchemy.exc import OperationalError, DatabaseError
from components.recon_utils import (
    map_status_column,
    map_tenant_id_column,
    merge_ebo_wallet_data,
)

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
            raise


def imps_service_function(start_date, end_date, service_name, df_excel):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    not_in_vendor = pd.DataFrame()
    status_column_db = f"{service_name}_STATUS"
    status_column_excel = "VENDOR_STATUS"
    Excel_count = len(df_excel)
    required_columns = [
        "ITI_ID",
        "REFID",
        "ACTUAL_AMOUNT",
        "REQUEST_AMOUNT",
        "VENDOR_AMOUNT",
        "VENDOR_STATUS",
        f"{service_name}_STATUS",
        "VENDOR_DATE",
        "SERVICE_DATE",
    ]

    query = text(
        """
       SELECT u.apna_id as ITI_ID,ait.utr_no as VENDOR_REFERENCE,ait.tot_amt as ACTUAL_AMOUNT,
    ait.trans_amt as REQUEST_AMOUNT,DATE(ait.post_dt) as SERVICE_DATE,ait.status as service_status
    from iti_portal.axis_imps_trans ait 
    LEFT JOIN iti_portal.users u on ait.users_id = u.id 
    WHERE DATE(ait.post_dt) BETWEEN :start_date AND :end_date AND ait.utr_no IS NOT NULL 
    AND ait.utr_no <> '' 
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

        status_mapping = {
            1: "success",
            0: "failed",
        }

        # Map status column
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        # Check and convert VENDOR_REFERENCE
        if "VENDOR_REFERENCE" in df_db.columns:
            df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
        Hub_count = df_db.shape[0]
        # Tenant ID mapping
        # df_db = map_tenant_id_column(df_db, "TENANT_ID")
        # Merge with EBO Wallet data
        # result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)
        # Preprocess dates
        if "VENDOR_DATE" in df_excel.columns:
            df_excel["VENDOR_DATE"] = pd.to_datetime(
                df_excel["VENDOR_DATE"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        if "SERVICE_DATE" in df_db.columns:
            df_db["SERVICE_DATE"] = pd.to_datetime(
                df_db["SERVICE_DATE"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

        def safe_column_select(df, columns):
            existing_cols = [col for col in columns if col in df.columns]
            return df[existing_cols].copy()

        refid_list = df_excel["REFID"].dropna().astype(str).str.strip()

        not_in_vendor = df_db[
            (~df_db["VENDOR_REFERENCE"].astype(str).str.strip().isin(refid_list))
            | (df_db["VENDOR_REFERENCE"].isna())
            | (df_db["VENDOR_REFERENCE"].astype(str).str.strip() == "")
        ].copy()

        not_in_vendor["CATEGORY"] = "NOT_IN_VENDOR"
        not_in_vendor = not_in_vendor.rename(columns={"VENDOR_REFERENCE": "REFID"})
        not_in_vendor = safe_column_select(not_in_vendor, required_columns)

        not_in_portal = df_excel[
            ~df_excel["REFID"].isin(df_db["VENDOR_REFERENCE"])
        ].copy()
        not_in_portal["CATEGORY"] = "NOT_IN_PORTAL"
        not_in_portal = safe_column_select(not_in_portal, required_columns)

        matched = df_db.merge(
            df_excel, left_on="VENDOR_REFERENCE", right_on="REFID", how="inner"
        ).copy()
        matched["CATEGORY"] = "MATCHED"
        matched = safe_column_select(matched, required_columns)
        # print(matched[matched['IHUB_LEDGER_STATUS'] == 'No'])
        # Mismatched
        mismatched = matched[
            matched[status_column_db].astype(str).str.lower()
            != matched[status_column_excel].astype(str).str.lower()
        ].copy()
        mismatched["CATEGORY"] = "MISMATCHED"
        mismatched = safe_column_select(mismatched, required_columns)

        def scenario_df(df, cond, category):
            out = df[cond].copy()
            out["CATEGORY"] = category
            return safe_column_select(out, required_columns)

        # Define scenario conditions
        scenarios = {
            "not_in_vendor": not_in_vendor,
            "not_in_portal": not_in_portal,
            "matched": matched,
            "mismatched": mismatched,
            "vend_ihub_succ": scenario_df(
                matched,
                (matched[status_column_excel].astype(str).str.lower() == "success")
                & (matched[status_column_db].astype(str).str.lower() == "success"),
                "VEND_IHUB_SUC",
            ),
            "vend_fail_ihub_succ": scenario_df(
                matched,
                (matched[status_column_excel].astype(str).str.lower() == "failed")
                & (matched[status_column_db].astype(str).str.lower() == "success"),
                "VEND_FAIL_IHUB_SUC",
            ),
            "vend_succ_ihub_fail": scenario_df(
                matched,
                (matched[status_column_excel].astype(str).str.lower() == "success")
                & (matched[status_column_db].astype(str).str.lower() == "failed"),
                "VEND_SUC_IHUB_FAIL",
            ),
            "ihub_vend_fail": scenario_df(
                matched,
                (
                    matched[status_column_excel]
                    .astype(str)
                    .str.lower()
                    .isin(["failed", "timed out"])
                )
                & (matched[status_column_db].astype(str).str.lower() == "failed"),
                "IHUB_FAIL_VEND_FAIL",
            ),
            "ihub_initiate_vend_succes": scenario_df(
                matched,
                (matched[status_column_excel].astype(str).str.lower() == "success")
                & (
                    matched[status_column_db]
                    .astype(str)
                    .str.lower()
                    .isin(["initiated", "inprogress", "pending"])
                ),
                "IHUB_INT_VEND_SUC",
            ),
            "ihub_initiate_vend_fail": scenario_df(
                matched,
                (
                    matched[status_column_excel]
                    .astype(str)
                    .str.lower()
                    .isin(["failed", "timed out"])
                )
                & (
                    matched[status_column_db]
                    .astype(str)
                    .str.lower()
                    .isin(["initiated", "inprogress", "pending"])
                ),
                "VEND_FAIL_IHUB_INT",
            ),
        }
        matched_success_status = matched[
            (matched[status_column_db].astype(str).str.lower() == "success")
            & (matched[status_column_excel].astype(str).str.lower() == "success")
        ]
        success_count = matched_success_status.shape[0]
        matched_failed_status = matched[
            (matched[status_column_db].astype(str).str.lower() == "failed")
            & (matched[status_column_excel].astype(str).str.lower() == "failed")
        ]
        failed_count = matched_failed_status.shape[0]
        combine_keys = [
            "not_in_vendor",
            "not_in_portal",
            "vend_fail_ihub_succ",
            "vend_succ_ihub_fail",
            "ihub_initiate_vend_succes",
            "ihub_initiate_vend_fail",
        ]
        dfs = [scenarios[k] for k in combine_keys]
        all_columns = set().union(*[df.columns for df in dfs])
        aligned_dfs = []
        for df in dfs:
            df_copy = df.copy()
            for col in all_columns - set(df_copy.columns):
                df_copy[col] = None
            df_copy = df_copy[list(all_columns)]
            aligned_dfs.append(df_copy)
        non_empty_dfs = [
            df for df in aligned_dfs if not df.empty and not df.isna().all().all()
        ]
        if not non_empty_dfs:
            logger.info("Filteration Ends")
            message = "Hurray there is no Mistmatch values in your DataSet..!"
            mapping = {
                "message": message,
                "Total_Success_count": success_count,
                "Total_Failed_count": failed_count,
                "Excel_value_count": Excel_count,
                "HUB_Value_count": Hub_count,
                "VEND_IHUB_SUC": scenarios["vend_ihub_succ"],
                "VEND_IHUB_FAIL": scenarios["ihub_vend_fail"],
            }
        else:
            logger.info("Filteration Ends")
            combined = pd.concat(non_empty_dfs, ignore_index=True)
            mapping = {
                "not_in_vendor": scenarios["not_in_vendor"],
                "combined": combined,
                "not_in_Portal": scenarios["not_in_portal"],
                "VEND_IHUB_SUC": scenarios["vend_ihub_succ"],
                "VEND_FAIL_IHUB_SUC": scenarios["vend_fail_ihub_succ"],
                "VEND_SUC_IHUB_FAIL": scenarios["vend_succ_ihub_fail"],
                "VEND_IHUB_FAIL": scenarios["ihub_vend_fail"],
                "IHUB_INT_VEND_SUC": scenarios["ihub_initiate_vend_succes"],
                "VEND_FAIL_IHUB_INT": scenarios["ihub_initiate_vend_fail"],
                "Total_Success_count": success_count,
                "Total_Failed_count": failed_count,
                "Excel_value_count": Excel_count,
                "HUB_Value_count": Hub_count,
            }
        return mapping

    except SQLAlchemyError as e:
        logger.error(f"Database error in imps_service_function(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in imps_service_function(): {e}")
    return result
