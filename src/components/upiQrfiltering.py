import pandas as pd
from db_connector import get_db_connection
from logger_config import logger
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import numpy as np
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
            if "Unique_ID" in df_excel:
                hub_data,initial_hub_data = UpiQr_Service(start_date, end_date, service_name)
                # print("Hub data",initial_hub_data["REFERENCE_NO"].to_list())
                df_excel["VENDOR_STATUS"] = (
                    df_excel["VENDOR_STATUS"]
                    .fillna("failed")
                    .apply(
                        lambda x: "success" if x.lower() == "authorised" else "failed"
                    )
                )
                result = filtering_Data(hub_data, initial_hub_data,df_excel, service_name)
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


def filtering_Data(df_db, initial_hub_data,df_excel, service_name):
    try:
        logger.info(f"Filteration Starts for {service_name} service")
        mapping = None
        Excel_count = len(df_excel)
        Vendor_success_count = Excel_count
        Hub_count = df_db.shape[0]
        df_db["SERVICE_DATE"] = pd.to_datetime(
            df_db["SERVICE_DATE"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        initial_hub_data["SERVICE_DATE"] = pd.to_datetime(
            initial_hub_data["SERVICE_DATE"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        df_excel["VENDOR_DATE"] = pd.to_datetime(
            df_excel["VENDOR_DATE"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        # df_excel["TRNSCTN_NMBR"] = df_excel["TRNSCTN_NMBR"].astype(str)
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
        initial_hub_data[columns_to_update] = initial_hub_data[columns_to_update].apply(
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
            "REFERENCE_NO",
            "IHUB_USERNAME",
            "TRANS_MODE",
            "HUB_AMOUNT",
            "VENDOR_AMOUNT",
            "SERVICE_DATE",
            "VENDOR_STATUS",
            "IHUB_MASTER_STATUS",
            f"{service_name}_STATUS",
            "EBO_WALLET_CREDIT",
            "TRANSACTION_TYPE",
        ]
        print(initial_hub_data.columns.tolist())
        bank_ref_not_updated = initial_hub_data[
            (
                (initial_hub_data["VENDOR_REFERENCE"].astype(str) == "0")
                | (initial_hub_data["VENDOR_REFERENCE"].astype(str).str.lower() == "nan")
            )
            & (~initial_hub_data["REFERENCE_NO"].isna())
        ].copy()
        bank_ref_not_updated = bank_ref_not_updated[bank_ref_not_updated["REFERENCE_NO"].isin(df_excel["Unique_ID"])].copy()
        bank_ref_not_updated["CATEGORY"] = "BANK_REF_NOT_UPDATED"
        bank_ref_not_updated = bank_ref_not_updated.rename(
            columns={"Settled_Amount": "AMOUNT", "Unique_ID": "REFERENCE_NO"}
        )
        bank_ref_not_updated = bank_ref_not_updated.merge(
            df_excel,
            left_on="REFERENCE_NO",
            right_on="Unique_ID",
            how="left",
            suffixes=("", "_VEND"),
        )
        print("Bank ref not updated",bank_ref_not_updated.columns.to_list())
        bank_ref_not_updated = safe_column_select(bank_ref_not_updated, required_columns)
        # 1 Filtering Data initiated in IHUB portal and not in Vendor Xl
        not_in_vendor = df_db[~df_db["REFERENCE_NO"].isin(df_excel["Unique_ID"])].copy()
        not_in_vendor["CATEGORY"] = "NOT_IN_VENDOR"
        not_in_vendor = safe_column_select(not_in_vendor, required_columns)
        # 2. Filtering Data Present in Vendor XL but Not in Ihub Portal
        # print(df_db["REFERENCE_NO"].to_list())
        not_in_portal = df_excel[
            ~df_excel["Unique_ID"].isin(initial_hub_data["REFERENCE_NO"])
        ].copy()
        not_in_portal["CATEGORY"] = "NOT_IN_PORTAL"
        not_in_portal = not_in_portal.rename(
            columns={"Settled_Amount": "AMOUNT", "Unique_ID": "REFERENCE_NO"}
        )
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
            "bank_ref_not_updated": bank_ref_not_updated,
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
                & (
                    matched["IHUB_MASTER_STATUS"]
                    .str.lower()
                    .isin(["initiated", "inprogress"])
                ),
                "IHUB_INT_VEND_SUC",
            ),
            "ihub_initiate_vend_fail": scenario_df(
                matched,
                (matched["VENDOR_STATUS"].str.lower() == "failed")
                & (
                    matched["IHUB_MASTER_STATUS"]
                    .str.lower()
                    .isin(["initiated", "inprogress"])
                ),
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
                "bank_ref_not_updated": scenarios["bank_ref_not_updated"],
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
                "Vendor_success_count": Vendor_success_count,
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

    # --- Query 1: AxisEpTransaction (HUB) ---
    query_ihub = text(
        """
        SELECT 
        aet.ReferenceNo AS REFERENCE_NO,
        aet.TransStatus AS IHUB_MASTER_STATUS, 
        aet.TransRemarks AS service_status, 
        aet.BankReferenceNo AS VENDOR_REFERENCE,
        DATE(aet.CreationTs) AS SERVICE_DATE,
        wls.TenantDetailId AS TENANT_ID,
        wls.EboUserName AS IHUB_USERNAME,
        aet.Amount AS HUB_AMOUNT,
        aet.RE2 AS TRANS_MODE
    FROM ihubcore.AxisEpTransaction aet
    LEFT JOIN ihubcore.WhiteLabelSession wls 
        ON wls.Id = aet.WhiteLabelSessionId
    WHERE aet.CreationTs  >= CONCAT(:start_date, ' 00:00:00')
        AND aet.CreationTs  <  DATE_ADD(CONCAT(:end_date, ' 00:00:00'), INTERVAL 1 DAY)
        AND  wls.CreationTs  >= CONCAT(:start_date, ' 00:00:00')
        AND wls.CreationTs  <  DATE_ADD(CONCAT(:end_date, ' 00:00:00'), INTERVAL 1 DAY)"""
    )

    # --- Query 2: EboWalletTransaction with manual refund included ---
    query_ebo = text(
        """
        SELECT 
            ewt.VendorReferenceId,
            ewt.IHubReferenceId,
            COALESCE(ewt.VendorReferenceId, ewt.IHubReferenceId) AS UNIQUE_REFERENCE,
            ewt.ServiceName,
            MAX(
                CASE 
                    WHEN ewt.Description IN (
                        'Wallet topup - Credit',
                        'Manual Refund Credit - Transaction - Credit'
                    ) THEN 'Yes'
                    ELSE 'No'
                END
            ) AS EBO_WALLET_CREDIT,
            CASE
                WHEN ewt.VendorReferenceId IS NOT NULL THEN 'AUTO_CREDIT'
                WHEN ewt.VendorReferenceId IS NULL AND ewt.IHubReferenceId IS NOT NULL THEN 'MANUAL_REFUND'
                ELSE 'UNKNOWN'
            END AS TRANSACTION_TYPE
        FROM tenantinetcsc.EboWalletTransaction ewt
        WHERE ewt.CreationTs  >= CONCAT(:start_date, ' 00:00:00')
        AND ewt.CreationTs  <  DATE_ADD(CONCAT(:end_date, ' 00:00:00'), INTERVAL 30 DAY)
          AND ewt.serviceName LIKE '%UPI%'
        GROUP BY ewt.VendorReferenceId, ewt.IHubReferenceId, ewt.ServiceName
    """
    )

    params = {"start_date": start_date, "end_date": end_date}

    try:
        # Execute queries
        df_hub = execute_sql_with_retry(query_ihub, params=params)
        df_ebo = execute_sql_with_retry(query_ebo, params=params)

        if df_hub.empty and df_ebo.empty:
            logger.warning(f"No data found for {service_name}")
            return pd.DataFrame()
        # print(df_hub["REFERENCE_NO"].to_list())
        # --- Prepare HUB dataframe ---
        df_hub[f"{service_name}_STATUS"] = df_hub["service_status"]
        df_hub.drop(columns=["service_status"], inplace=True)
        # Tenant mapping
        tenant_Id_mapping = {1: "INET-CSC", 2: "ITI-ESEVA", 3: "UPCB"}
        df_hub["TENANT_ID"] = (
            df_hub["TENANT_ID"].map(tenant_Id_mapping).fillna(df_hub["TENANT_ID"])
        )
        # Clean merge keys
        for col in ["VENDOR_REFERENCE", "REFERENCE_NO"]:
            df_hub[col] = (
                df_hub[col]
                .astype(str)
                .str.strip()
                .replace({"None": np.nan, "nan": np.nan, "NULL": np.nan})
            )
        df_ebo["UNIQUE_REFERENCE"] = (
            df_ebo["UNIQUE_REFERENCE"]
            .astype(str)
            .str.strip()
            .replace({"None": np.nan, "nan": np.nan, "NULL": np.nan})
        )

        # --- Deduplicate EBO data by UNIQUE_REFERENCE ---
        # df_ebo = df_ebo.groupby("UNIQUE_REFERENCE", as_index=False).agg({
        #     "EBO_WALLET_CREDIT": "max",
        #     "TRANSACTION_TYPE": "first",
        #     "ServiceName": "first",
        # })

        # Split by transaction type
        df_auto = df_ebo[df_ebo["TRANSACTION_TYPE"] == "AUTO_CREDIT"].copy()
        df_manual = df_ebo[df_ebo["TRANSACTION_TYPE"] == "MANUAL_REFUND"].copy()

        # --- Merge AUTO_CREDIT ---
        merged_auto = pd.merge(
            df_hub,
            df_auto,
            how="left",
            left_on="VENDOR_REFERENCE",
            right_on="UNIQUE_REFERENCE",
            suffixes=("", "_EBO"),
        )
        merged_auto["MERGE_TYPE"] = "AUTO_CREDIT"
        merged_auto = merged_auto.drop_duplicates(subset=["VENDOR_REFERENCE"])

        # --- Merge MANUAL_REFUND ---
        merged_manual = pd.merge(
            df_hub,
            df_manual,
            how="left",
            left_on="REFERENCE_NO",
            right_on="UNIQUE_REFERENCE",
            suffixes=("", "_EBO"),
        )
        merged_manual["MERGE_TYPE"] = "MANUAL_REFUND"
        merged_manual = merged_manual.drop_duplicates(subset=["REFERENCE_NO"])
        
        # --- Combine final results ---
        result = pd.concat([merged_auto, merged_manual], ignore_index=True)
        result["SERVICE_NAME"] = result["ServiceName"].fillna(service_name)
        result.drop(columns=["ServiceName"], inplace=True, errors="ignore")
        # print("before removing Dup",result["REFERENCE_NO"].to_list())
        # --- Drop duplicate VENDOR_REFERENCE rows having both NaN in TRANSACTION_TYPE and EBO_WALLET_CREDIT ---
        def drop_invalid_duplicates(df):
            # Mark rows where both columns are NaN
            df["_both_na"] = (
                df["TRANSACTION_TYPE"].isna() & df["EBO_WALLET_CREDIT"].isna()
            )
            # Sort so that valid rows come first within each VENDOR_REFERENCE
            df = df.sort_values(
                by=["VENDOR_REFERENCE", "_both_na"], ascending=[True, True]
            )
            # Drop duplicates keeping the first (valid if exists)
            df = df.drop_duplicates(subset=["VENDOR_REFERENCE"], keep="first")
            # Remove helper column
            df = df.drop(columns="_both_na")
            return df

        result = drop_invalid_duplicates(result)
        # print("After removing Dup",result["REFERENCE_NO"].to_list())
        # Debug output
        # print(result[['EBO_WALLET_CREDIT', 'TRANSACTION_TYPE', 'MERGE_TYPE']].head(5))
        logger.info(f"Fetched and merged {len(result)} records for {service_name}")
        return result, df_hub

    except SQLAlchemyError as e:
        logger.error(f"Database error in UpiQr_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in UpiQr_Service(): {e}")

    return result
