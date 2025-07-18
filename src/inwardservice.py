# --- DRY Helper Functions are now in recon_utils.py ---
from recon_utils import map_status_column, map_tenant_id_column, merge_ebo_wallet_data


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
from filteration_process import unified_filtering_data

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


def inward_service_selection(
    start_date, end_date, service_name, transaction_type, df_excel
):
    try:
        logger.info(f"Entering Reconciliation for {service_name} Service")

        if service_name == "AEPS":
            if "REFID" in df_excel:
                df_excel["REFID"] = df_excel["REFID"].astype(str)
                logger.info("AEPS service: Column 'SERIALNUMBER' renamed to 'REFID'")
                # tenant_service_id = 159
                # Hub_service_id = 7374
                # Hub_service_id = ",".join(str(x) for x in Hub_service_id)
                hub_data = aeps_Service(
                    start_date, end_date, service_name, transaction_type
                )
                # tenant_data = tenant_filtering(
                #     start_date, end_date, tenant_service_id, Hub_service_id
                # )
                result = filtering_Data(hub_data, df_excel, service_name)
            else:
                logger.warning("Wrong File Uploaded in AEPS Function")
                message = "Wrong File Updloaded...!"
                return message
        elif service_name == "MATM":
            if "REFID" in df_excel:
                df_cleaned = df_excel[
                    ~(
                        df_excel["orderID"].isna()
                        & df_excel["REFID"].isna()
                        & df_excel["Device"].isna()
                    )
                ].copy()
                # df_cleaned["REFID"] = df_excel["REFID"].astype(str)
                df_cleaned["VENDOR_STATUS"] = (
                    df_cleaned["VENDOR_STATUS"]
                    .replace({"Transaction Successful.": "success"})
                    .fillna("failed")
                )
                hub_data = matm_Service(start_date, end_date, service_name)
                result = filtering_Data(hub_data, df_cleaned, service_name)
            else:
                logger.warning("Wrong File Uploaded in MATM function")
                message = "Wrong File Updloaded...!"
                return message
        else:
            logger.warning("InwardService  function selection Error ")
            message = "Service Name Error..!"
            return message

        return result
    except Exception as e:
        print("Error in inward function :", e)


def getServiceId(MasterServiceId, MasterVendorId):
    logger.info("Entered Get Service ID Function")
    result = None

    query = text(
        """
        SELECT vssm.id FROM ihubcore.VendorSubServiceMapping vssm join ihubcore.MasterSubService mss  on vssm.MasterSubServiceId =mss.Id
        join ihubcore.MasterService ms on ms.id=mss.MasterServiceId
        join ihubcore.MasterVendor mv on vssm.MasterVendorId =mv.id
        where ms.Id = :MasterServiceId and mv.id = :MasterVendorId
        """
    )
    params = {"MasterServiceId": MasterServiceId, "MasterVendorId": MasterVendorId}
    try:
        df = execute_sql_with_retry(
            query,
            params=params,
        )

        if df.empty:
            logger.warning("No values returned from Get Service Id Function")
            return pd.DataFrame()
        else:
            result = df
    except SQLAlchemyError as e:
        logger.error(f"Database error :{e}")
    except Exception as e:
        logger.error(f"Unexpected error : {e}")
    return result


# Use the unified filtering function with parameters matching the old logic
def filtering_Data(df_db, df_excel, service_name):
    required_columns = [
        "CATEGORY",
        "VENDOR_DATE",
        "TENANT_ID",
        "IHUB_REFERENCE",
        "REFID",
        "IHUB_USERNAME",
        "AMOUNT",
        "VENDOR_STATUS",
        "IHUB_MASTER_STATUS",
        f"{service_name}_STATUS",
        "SERVICE_DATE",
        "IHUB_LEDGER_STATUS",
        "TRANSACTION_CREDIT",
        "TRANSACTION_DEBIT",
        "COMMISSION_CREDIT",
        "COMMISSION_REVERSAL",
    ]
    amount_column_map = {
        "AEPS": "AEPS_AMOUNT",
        "MATM": "MATM_AMOUNT",
    }
    status_mapping_db = {
        0: "initiated",
        1: "success",
        2: "failed",
        3: "inprogress",
        4: "partial success",
    }
    return unified_filtering_data(
        df_db,
        df_excel,
        service_name,
        status_column_db="IHUB_MASTER_STATUS",
        status_column_excel="VENDOR_STATUS",
        required_columns=required_columns,
        amount_column_map=amount_column_map,
        ledger_status_col="IHUB_LEDGER_STATUS",
        status_mapping_db=status_mapping_db,
        logger_obj=logger,
    )


# Ebo Wallet Amount and commission  Debit credit check function  -------------------------------------------
def get_ebo_wallet_data(start_date, end_date):
    logger.info("Fetching Data from EBO Wallet Transaction")
    ebo_df = None
    query = text(
        """
    SELECT  
    ewt.IHubReferenceId,
    COALESCE(MAX(CASE WHEN ewt.Description = 'Transaction - Credit' THEN 'Yes' END), 'No') AS TRANSACTION_CREDIT,
    COALESCE(MAX(CASE WHEN ewt.Description = 'Transaction - Debit' THEN 'Yes' END), 'No') AS TRANSACTION_DEBIT,
    COALESCE(MAX(CASE WHEN ewt.Description = 'Commission Added' THEN 'Yes' END), 'No') AS COMMISSION_CREDIT,
    COALESCE(MAX(CASE WHEN ewt.Description = 'Commission - Reversal' THEN 'Yes' END), 'No') AS COMMISSION_REVERSAL
    FROM
        ihubcore.MasterTransaction mt2
    JOIN tenantinetcsc.MasterTransaction mt 
        ON mt.TransactionRefNumIHub = mt2.TransactionRefNum
    JOIN tenantinetcsc.EboWalletTransaction ewt 
        ON mt.Id = ewt.MasterTransactionsId
    WHERE
        DATE(mt2.CreationTs) BETWEEN :start_date AND :end_date AND
        DATE(mt.CreationTs) BETWEEN :start_date AND :end_date 
        AND ewt.IHubReferenceId IS NOT NULL
        AND ewt.IHubReferenceId <> ''
    GROUP BY 
        ewt.IHubReferenceId
    """
    )
    # hi
    try:
        # Call the retry-enabled query executor
        ebo_df = execute_sql_with_retry(
            query, params={"start_date": start_date, "end_date": end_date}
        )
        if ebo_df.empty:
            logger.warning("No data returned from EBO Wallet table.")
    except SQLAlchemyError as e:
        logger.error(f"Database error in EBO Wallet Query: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in EBO Wallet Query Execution: {e}")

    return ebo_df

# ----------------------------------------------------------------------------------
# Aeps function
def aeps_Service(start_date, end_date, service_name, transaction_type):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()

    query = text(
        """
        SELECT 
            mt2.TransactionRefNum AS IHUB_REFERENCE,
            par.ReferenceNo  AS VENDOR_REFERENCE,
            mt2.TenantDetailId as TENANT_ID,
            u.UserName as IHUB_USERNAME,
            mt2.TransactionStatus AS IHUB_MASTER_STATUS,
            pat.CreationTs AS SERVICE_DATE,
            pat.TransStatus AS service_status,
            pat.Amount AS AEPS_AMOUNT,
            CASE 
                WHEN a.IHubReferenceId IS NOT NULL THEN 'Yes'
                ELSE 'No'
            END AS IHUB_LEDGER_STATUS
        FROM ihubcore.MasterTransaction mt2 
        LEFT JOIN ihubcore.MasterSubTransaction mst
            ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN ihubcore.PsAepsTransaction pat 
            ON pat.MasterSubTransactionId = mst.Id
            JOIN ihubcore.PsAepsRequest par on par.id=pat.RequestId 
        LEFT JOIN tenantinetcsc.EboDetail ed
            ON mt2.EboDetailId = ed.Id
        LEFT JOIN tenantinetcsc.`User` u
            ON u.id = ed.UserId
        LEFT JOIN (
            SELECT DISTINCT iwt.IHubReferenceId AS IHubReferenceId
            FROM ihubcore.IHubWalletTransaction iwt
            WHERE DATE(iwt.CreationTs) BETWEEN :start_date AND CURRENT_DATE()
        ) a 
            ON a.IHubReferenceId = mt2.TransactionRefNum
        WHERE pat.TransMode = :transaction_type
        AND DATE(pat.CreationTs) BETWEEN :start_date AND :end_date
    """
    )

    params = {
        "start_date": start_date,
        "end_date": end_date,
        "transaction_type": transaction_type,
    }

    try:
        # Safe query execution with retry
        df_db = execute_sql_with_retry(query, params=params)

        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()

        # Map status codes to human-readable strings
        status_mapping = {
            3: "inprocess",
            2: "timeout",
            1: "success",
            255: "initiated",
            254: "failed",
            0: "failed",
        }

        # Map status column
        df_db = map_status_column(
            df_db, "service_status",  status_mapping,new_column=f"{service_name}_STATUS",
            drop_original=True)
        # Check and convert VENDOR_REFERENCE
        if "VENDOR_REFERENCE" in df_db.columns:
            df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
        # Tenant ID mapping
        df_db = map_tenant_id_column(df_db, "TENANT_ID")
        # Merge with EBO Wallet data
        result = merge_ebo_wallet_data(df_db, start_date, end_date,get_ebo_wallet_data)

    except SQLAlchemyError as e:
        logger.error(f"Database error in aeps_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in aeps_Service(): {e}")

    return result


# -------------------------------------------------------------------------
# M-ATM SERVICE FUNCTION
def matm_Service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        """ SELECT 
            mt2.TransactionRefNum AS IHUB_REFERENCE,
            iwmt.Rrn AS VENDOR_REFERENCE,
            mt2.TenantDetailId as TENANT_ID,
            u.UserName as IHUB_USERNAME,
            mt2.TransactionStatus AS IHUB_MASTER_STATUS,
            iwmt.CreationTs AS SERVICE_DATE,
            iwmt.Amount AS MATM_AMOUNT,
            iwmt.TransStatusType  AS service_status,
            CASE 
                WHEN a.IHubReferenceId IS NOT NULL THEN 'Yes'
                ELSE 'No'
            END AS IHUB_LEDGER_STATUS,
            CASE
                WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                ELSE 'No'
            END AS TENANT_LEDGER_STATUS
        FROM ihubcore.MasterTransaction mt2 
        LEFT JOIN ihubcore.MasterSubTransaction mst
            ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN ihubcore.ImWalletMatmTransaction iwmt  
            ON iwmt.MasterSubTransactionId = mst.Id
        LEFT JOIN tenantinetcsc.EboDetail ed
            ON mt2.EboDetailId = ed.Id
        LEFT JOIN tenantinetcsc.`User` u
            ON u.id = ed.UserId
        LEFT JOIN (
            SELECT DISTINCT iwt.IHubReferenceId AS IHubReferenceId
            FROM ihubcore.IHubWalletTransaction iwt
            WHERE DATE(iwt.CreationTs) BETWEEN :start_date AND :end_date
        ) a 
        ON a.IHubReferenceId = mt2.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) twt ON twt.IHubReferenceId = mt2.TransactionRefNum 
        WHERE DATE(iwmt.CreationTs) BETWEEN :start_date AND :end_date
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
            0: "unknown",
            1: "failed",
            2: "success",
        }
        # Map status column
        df_db = map_status_column(
            df_db, "service_status",  status_mapping,new_column=f"{service_name}_STATUS",
            drop_original=True)
        # Check and convert VENDOR_REFERENCE
        if "VENDOR_REFERENCE" in df_db.columns:
            df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
        # Tenant ID mapping
        df_db = map_tenant_id_column(df_db, "TENANT_ID")
        # Merge with EBO Wallet data
        result = merge_ebo_wallet_data(df_db, start_date, end_date,get_ebo_wallet_data)

    except SQLAlchemyError as e:
        logger.error(f"Database error in Matm_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in Matm_Service(): {e}")

    return result


# ----------------------------------------------------------------------------------
#
