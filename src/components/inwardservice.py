# --- DRY Helper Functions are now in recon_utils.py ---
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
    if transaction_type == "2":
        query = text(
            """
        SELECT 
            mt2.TransactionRefNum AS IHUB_REFERENCE,
            par.ReferenceNo  AS VENDOR_REFERENCE,
            mt2.TenantDetailId as TENANT_ID,
            mt2.CreationUserId as IHUB_USERNAME,
            mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
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
        LEFT JOIN (
            SELECT DISTINCT iwt.IHubReferenceId AS IHubReferenceId
            FROM ihubcore.IHubWalletTransaction iwt
            WHERE DATE(iwt.CreationTs) BETWEEN :start_date AND :end_date
        ) a 
            ON a.IHubReferenceId = mt2.TransactionRefNum
        WHERE pat.TransMode = :transaction_type
        AND DATE(pat.CreationTs) BETWEEN :start_date AND :end_date
    """
        )
    elif transaction_type == "3":
        query = text(
            """
        SELECT 
            mt2.TransactionRefNum AS IHUB_REFERENCE,
            par.ReferenceNo  AS VENDOR_REFERENCE,
            mt2.TenantDetailId as TENANT_ID,
            mt2.CreationUserId as IHUB_USERNAME,
            mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
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
        LEFT JOIN (
            SELECT DISTINCT iwt.IHubReferenceId AS IHubReferenceId  
            FROM ihubcore.IHubWalletTransaction iwt
            WHERE DATE(iwt.CreationTs) BETWEEN :start_date AND :end_date
        ) a
            ON a.IHubReferenceId = mt2.TransactionRefNum
        WHERE pat.TransMode = :transaction_type and
        DATE(pat.CreationTs) BETWEEN :start_date AND :end_date
    """
        )
    else:
        message = f"Invalid transaction_type '{transaction_type}' for service '{service_name}'"
        logger.error(message)
        return message

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
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        # Check and convert VENDOR_REFERENCE
        if "VENDOR_REFERENCE" in df_db.columns:
            df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
        # Tenant ID mapping
        df_db = map_tenant_id_column(df_db, "TENANT_ID")
        # Merge with EBO Wallet data
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)

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
            mt2.CreationUserId as IHUB_USERNAME,
            mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
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
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        # Check and convert VENDOR_REFERENCE
        if "VENDOR_REFERENCE" in df_db.columns:
            df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
        # Tenant ID mapping
        df_db = map_tenant_id_column(df_db, "TENANT_ID")
        # Merge with EBO Wallet data
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)

    except SQLAlchemyError as e:
        logger.error(f"Database error in Matm_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in Matm_Service(): {e}")

    return result


# ----------------------------------------------------------------------------------
