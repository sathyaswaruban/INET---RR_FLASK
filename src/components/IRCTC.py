import pandas as pd
import logging
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


####
@retry(**DB_RETRY_CONFIG)
def execute_sql_with_retry(query, params=None):
    logger.info("Entered helper function to execute SQL with retry logic")

    try:
        with engine.connect() as connection:
            result = connection.execute(query, params or {})
            df = pd.DataFrame(
                result.fetchall(), columns=result.keys()
            )  # Fully fetches all rows
            return df
    except Exception as e:
        logger.error(f"Error during SQL execution: {e}")
        raise


def irctc(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    transaction_credit_descriptions = [
        "Transaction - Credit",
        "Transaction - Credit due to failure",
        "Transaction - Refund",
        "Manual Refund Credit - Transaction - Credit",
    ]

    # Common descriptions for other fields
    transaction_debit_descriptions = [
        "Transaction - Debit",
        "Manual Refund Debit - Transaction - Debit",
    ]
    commission_credit_descriptions = [
        "Commission Added",
        "Manual Refund Credit - Commission - Added",
    ]
    commission_reversal_descriptions = [
        "Commission - Reversal",
        "Commission Reversal",
        "Manual Refund Debit - Commission - Reversal",
    ]
    query = text(
        """
           SELECT  
                mt2.TransactionRefNum AS IHUB_REFERENCE_ID,
                ewt.MasterTransactionsId AS TNT_MASTER_TRANSACTION_ID,
                ewt.VendorReferenceId as VENDOR_REF_ID,
                 MAX(CASE WHEN ewt.Description IN (
                    'Transaction - Debit','Manual Refund Debit - Transaction - Debit'
                ) THEN ewt.DebitAmount ELSE 0 END) AS DEBIT_AMOUNT,
                MAX(CASE WHEN ewt.Description IN (
                    'Transaction - Credit', 'Transaction - Credit due to failure', 'Transaction - Refund',
                    'Manual Refund Credit - Transaction - Credit'
                ) THEN ewt.CreditAmount ELSE 0 END) AS CREDIT_AMOUNT,
                MAX(CASE WHEN ewt.Description IN :transaction_credit_descriptions THEN 'Yes' ELSE 'No' END) AS TRANSACTION_CREDIT,
                MAX(CASE WHEN ewt.Description IN :transaction_debit_descriptions THEN 'Yes' ELSE 'No' END) AS TRANSACTION_DEBIT,
                MAX(CASE WHEN ewt.Description IN :commission_credit_descriptions THEN 'Yes' ELSE 'No' END) AS COMMISSION_CREDIT,
                MAX(CASE WHEN ewt.Description IN :commission_reversal_descriptions THEN 'Yes' ELSE 'No' END) AS COMMISSION_REVERSAL
            FROM ihubcore.MasterTransaction mt2
            JOIN tenantinetcsc.EboWalletTransaction ewt
                ON mt2.TenantMasterTransactionId = ewt.MasterTransactionsId
            WHERE mt2.CreationTs >= CONCAT(:start_date, ' 00:00:00')
            AND mt2.CreationTs <  DATE_ADD(CONCAT(:end_date, ' 00:00:00'), INTERVAL 1 DAY)
            AND ewt.CreationTs >= CONCAT(:start_date, ' 00:00:00')
            AND ewt.CreationTs <=  DATE_ADD(CONCAT(:end_date, ' 00:00:00'), INTERVAL 30 DAY)
            AND ewt.ServiceName LIKE :db_service_name
            GROUP BY ewt.MasterTransactionsId             
"""
    )

    try:
        irctc_df = execute_sql_with_retry(
            query,
            params={
                "start_date": start_date,
                "end_date": end_date,
                "db_service_name": "%IRCTC%",
                "transaction_credit_descriptions": tuple(
                    transaction_credit_descriptions
                ),
                "transaction_debit_descriptions": tuple(transaction_debit_descriptions),
                "commission_credit_descriptions": tuple(commission_credit_descriptions),
                "commission_reversal_descriptions": tuple(
                    commission_reversal_descriptions
                ),
            },
        )
        if irctc_df.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()
        print(irctc_df.shape[0])
        irctc_df = irctc_df.to_dict(orient="records")
    except SQLAlchemyError as e:
        logger.error(f"Database error in bbps_data_entry(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in bbps_data_entry(): {e}")
    return {"irctc_data": irctc_df}
