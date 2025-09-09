import pandas as pd
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


def upiQr_service_selection(start_date, end_date, service_name, df_excel):
    try:
        logger.info(f"Entering Reconciliation for {service_name} Service")
        if service_name == "UPIQR":
            if "TRNSCTN_NMBR" in df_excel:
                hub_data = UpiQr_Service(start_date, end_date, service_name)
                df_excel["VENDOR_STATUS"] = df_excel["VENDOR_STATUS"].fillna("failed").apply(lambda x: "success" if x.lower() == "authorised" else "failed")
                result = filtering_Data(hub_data, df_excel, service_name)
            else:
                logger.warning("Wrong File Uploaded for UPIQR function")
                message = "Wrong File Updloaded...!"
                return message
        else:
            logger.warning("InwardService  function selection Error ")
            message = "Service Name Error..!"
            return message

        return result
    except Exception as e:
        print("Error in inward function :", e)


def filtering_Data(df_db, df_excel, service_name):
    try:
        logger.info(f"Filteration Starts for {service_name} service")
        mapping = None
        Excel_count = len(df_excel)
        Hub_count = df_db.shape[0]
        df_db["SERVICE_DATE"] = pd.to_datetime(
            df_db["SERVICE_DATE"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        df_excel["VENDOR_DATE"] = pd.to_datetime(
            df_excel["VENDOR_DATE"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        status_mapping = {
            0: "success",
            5: "inprogress",
            7: "failed",
            255: "initiated",
        }

        columns_to_update = ["IHUB_MASTER_STATUS"]
        df_db[columns_to_update] = df_db[columns_to_update].apply(
            lambda x: x.map(status_mapping).fillna(x)
        )

        columns_to_update = ["IHUB_MASTER_STATUS"]
        df_db[columns_to_update] = df_db[columns_to_update].apply(
            lambda x: x.map(status_mapping).fillna(x)
        )

        # tenant_data["TENANT_STATUS"] = tenant_data["TENANT_STATUS"].apply(
        #     lambda x: status_mapping.get(x, x)
        # )

        # function to select only required cols and make it as df
        def safe_column_select(df, columns):
            existing_cols = [col for col in columns if col in df.columns]
            return df[existing_cols].copy()

        # Required columns that to be sent as result to UI
        required_columns = [
            "CATEGORY",
            "VENDOR_DATE",
            "TENANT_ID",
            "VENDOR_REFERENCE",
            "REFID",
            "IHUB_USERNAME",
            "TRANS_MODE",
            "AMOUNT",
            "SERVICE_DATE",
            "VENDOR_STATUS",
            "IHUB_MASTER_STATUS",
            f"{service_name}_STATUS",
            "TenantDB_wallettopup_status",
            "Hub_Tntwallettopup_status",
        ]
        # 1 Filtering Data initiated in IHUB portal and not in Vendor Xl
        not_in_vendor = df_db[~df_db["VENDOR_REFERENCE"].isin(df_excel["REFID"])].copy()
        not_in_vendor["CATEGORY"] = "NOT_IN_VENDOR"
        not_in_vendor = safe_column_select(not_in_vendor, required_columns)
        # 2. Filtering Data Present in Vendor XL but Not in Ihub Portal
        not_in_portal = df_excel[
            ~df_excel["REFID"].isin(df_db["VENDOR_REFERENCE"])
        ].copy()
        not_in_portal["CATEGORY"] = "NOT_IN_PORTAL"
        not_in_portal = not_in_portal.rename(columns={"Settled_Amount": "AMOUNT"})
        not_in_portal = safe_column_select(not_in_portal, required_columns)

        # 4. Filtering Data that matches in both Ihub Portal and Vendor Xl as : Matched
        matched = df_db.merge(
            df_excel, left_on="VENDOR_REFERENCE", right_on="REFID", how="inner"
        ).copy()
        matched["CATEGORY"] = "MATCHED"

        matched = safe_column_select(matched, required_columns)
        # 5. Filtering Data that Mismatched in both Ihub Portal and Vendor Xl as : Mismatched
        matched[f"{service_name}_STATUS"] = matched[f"{service_name}_STATUS"].astype(
            str
        )
        matched["VENDOR_STATUS"] = matched["VENDOR_STATUS"].astype(str)
        # print(matched.columns.tolist())

        mismatched = matched[
            matched[f"{service_name}_STATUS"].str.lower()
            != matched["VENDOR_STATUS"].str.lower()
        ].copy()

        mismatched["CATEGORY"] = "MISMATCHED"
        mismatched = safe_column_select(mismatched, required_columns)
        # 6. Getting total count of success and failure data
        matched_success_status = matched[
            (matched[f"{service_name}_STATUS"].str.lower() == "success")
            & (matched["VENDOR_STATUS"].str.lower() == "success")
        ]
        def scenario_df(df, cond, category):
            out = df[cond].copy()
            out["CATEGORY"] = category
            return safe_column_select(out, required_columns)

        scenarios = {
            "not_in_vendor": not_in_vendor,
            "not_in_portal": not_in_portal,
            "vend_ihub_succ": scenario_df(
                matched,
                (matched["VENDOR_STATUS"].str.lower() == "success")
                & (matched["IHUB_MASTER_STATUS"].str.lower() == "success"),
                "VEND_IHUB_SUC",
            ),
            "vend_ihub_fail": scenario_df(
                matched,
                (matched["VENDOR_STATUS"].str.lower() == "failed")
                & (matched["IHUB_MASTER_STATUS"].str.lower() == "failed"),
                "VEND_IHUB_FAIL",
            ),
            "vend_fail_ihub_succ": scenario_df(
                        matched,
                (matched["VENDOR_STATUS"].str.lower() == "failed")
                & (matched["IHUB_MASTER_STATUS"].str.lower() == "success"),
                "VEND_FAIL_IHUB_SUC",
            ),
            "vend_succ_ihub_fail": scenario_df(
                matched,
                (matched["VENDOR_STATUS"].str.lower() == "success")
                & (matched["IHUB_MASTER_STATUS"].str.lower() == "failed"),
                "VEND_SUC_IHUB_FAIL",
            ),
            "ihub_initiate_vend_succes": scenario_df(
                matched,
                (matched["VENDOR_STATUS"].str.lower() == "success")
                & (matched["IHUB_MASTER_STATUS"].str.lower().isin(["initiated", "inprogress"])),
                "IHUB_INT_VEND_SUC",
            ),
            "ihub_initiate_vend_fail": scenario_df(
                matched,
                (matched["VENDOR_STATUS"].str.lower() == "failed")
                & (matched["IHUB_MASTER_STATUS"].str.lower().isin(["initiated", "inprogress"])),
                "VEND_FAIL_IHUB_INT",
            ),
        }

        # Count success and failure
        matched_success_status = matched[
            (matched[f"{service_name}_STATUS"].str.lower() == "success")
            & (matched["VENDOR_STATUS"].str.lower() == "success")
        ]
        success_count = matched_success_status.shape[0]

        matched_failed_status = matched[
            (matched[f"{service_name}_STATUS"].str.lower() == "failed")
            & (matched["VENDOR_STATUS"].str.lower() == "failed")
        ]
        failed_count = matched_failed_status.shape[0]

        # Align and combine
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
            return {
                "message": message,
                "Total_Success_count": success_count,
                "Total_Failed_count": failed_count,
                "matched": matched,
            }
        else:
            logger.info("Filteration Ends")
            combined = pd.concat(non_empty_dfs, ignore_index=True)
            mapping = {
                "not_in_vendor": scenarios["not_in_vendor"],
                "combined": combined,
                "not_in_Portal": scenarios["not_in_portal"],
                "VEND_FAIL_IHUB_SUC": scenarios["vend_fail_ihub_succ"],
                "VEND_SUC_IHUB_FAIL": scenarios["vend_succ_ihub_fail"],
                "IHUB_INT_VEND_SUC": scenarios["ihub_initiate_vend_succes"],
                "VEND_FAIL_IHUB_INT": scenarios["ihub_initiate_vend_fail"],
                "Total_Success_count": success_count,
                "Total_Failed_count": failed_count,
                "Excel_value_count": Excel_count,
                "HUB_Value_count": Hub_count,
                "VEND_IHUB_SUC": scenarios["vend_ihub_succ"],
                "VEND_IHUB_FAIL": scenarios["vend_ihub_fail"],
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
def UpiQr_Service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        """ SELECT 
    aet.ReferenceNo  AS VENDOR_REFERENCE,
    aet.TransStatus as IHUB_MASTER_STATUS, 
    aet.TransRemarks as service_status, 
    DATE(aet.CreationTs) AS SERVICE_DATE,
    wls.TenantDetailId as TENANT_ID,
    wls.EboUserName as IHUB_USERNAME,
    aet.Amount AS AMOUNT,
    aet.RE2 as TRANS_MODE,
    CASE 
        WHEN Tenantwalletcheck.BankReferenceNo IS NOT NULL THEN 'YES'
        ELSE 'NO'
    END AS TenantDB_wallettopup_status,
    CASE 
    	when twt.id IS NOT NULL THEN 'YES'
    	ELSE 'NO'
    END as Hub_Tntwallettopup_status    
    FROM ihubcore.AxisEpTransaction aet 
    LEFT JOIN ihubcore.WhiteLabelSession wls 
    ON wls.Id = aet.WhiteLabelSessionId
    LEFT JOIN (
    SELECT wt.BankReferenceNo FROM tenantinetcsc.WalletTopup wt 
    WHERE DATE(wt.creationts) BETWEEN :start_date AND :end_date
    UNION ALL 
    SELECT wt2.BankReferenceNo FROM tenantupcb.WalletTopup wt2 
    WHERE DATE(wt2.creationts) BETWEEN :start_date AND :end_date
    UNION ALL 
    SELECT wt3.BankReferenceNo FROM tenantiticsc.WalletTopup wt3 
    WHERE DATE(wt3.creationts) BETWEEN :start_date AND :end_date
    ) AS Tenantwalletcheck
    ON CAST(aet.BankReferenceNo AS CHAR) = CAST(Tenantwalletcheck.BankReferenceNo AS CHAR)
    left join (select twt2.id  from ihubcore.TenantWalletTopup twt2 
    where DATE(twt2.creationts) BETWEEN :start_date AND :end_date) twt
    on twt.id = aet.TenantWalletTopupId 
    WHERE 
    DATE(aet.creationts) BETWEEN  :start_date AND :end_date
    AND DATE(wls.creationts) BETWEEN :start_date AND :end_date
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

        df_db[f"{service_name}_STATUS"] = df_db["service_status"]
        df_db.drop(columns=["service_status"], inplace=True)
        df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
        # df_db["AMOUNT"] = pd.to_numeric(df_db["AMOUNT"], errors="coerce").fillna(0)
        # Tenant ID mapping
        tenant_Id_mapping = {
            1: "INET-CSC",
            2: "ITI-ESEVA",
            3: "UPCB",
        }
        df_db["TENANT_ID"] = (
            df_db["TENANT_ID"].map(tenant_Id_mapping).fillna(df_db["TENANT_ID"])
        )
        # Merge with EBO Wallet data

        result = df_db

    except SQLAlchemyError as e:
        logger.error(f"Database error in UPIQR_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in UPIQR_Service(): {e}")

    return result
