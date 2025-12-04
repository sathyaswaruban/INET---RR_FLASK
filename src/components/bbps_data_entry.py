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


def bbps_data_entry(start_date, end_date, service_name, df_excel):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        """
       SELECT bbf.Id as BBPS_BillFetchId,bbf.BBPS_BillerDetailId,bbf.CustomerMobile,bbf.BillAmount as Amount,bbf.CreationTs,bbf.HeadReferenceId as HeadReferenceId_db,bbf.CustomerData,bbf.CustomerName,bbf.Request,bbf.Response,bbf.CreationUserId,bbf.EncryptedRequest,bbf.EncryptedResponse from ihubcore.BBPS_BillFetch bbf where DATE(bbf.CreationTs) BETWEEN :start_date and :end_date                 
"""
    )
    query2 = text(
        """
        SELECT mst.id as MST_ID , mst.TransRefNumVendorSub as VEND_ID from ihubcore.MasterSubTransaction mst  
        where TransRefNumVendorSub LIKE "%KM%" AND DATE(CreationTs) BETWEEN :start_date and :end_date
                    """
    )
    params = {"start_date": start_date, "end_date": end_date}
    try:
        df_db = execute_sql_with_retry(query, params=params)
        df_mst = execute_sql_with_retry(query2, params=params)
        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()

        result = pd.merge(
            df_excel[
                ["TxnRefId", "HeadReferenceId", "TransactionStatusType"]
            ],  # only needed cols from Excel
            df_db,
            left_on="HeadReferenceId",
            right_on="HeadReferenceId_db",
            how="inner",
        )
        result = pd.merge(
            result,
            df_mst,
            left_on="TxnRefId",
            right_on="VEND_ID",
            how="inner",
        )
        status_mapping = {
            "Successful": 1,
            "Transaction timed out": 2,
        }
        result["TransactionStatusType"] = result["TransactionStatusType"].apply(
            lambda x: status_mapping.get(x, x)
        )
        result["CreationTs"] = result["CreationTs"].apply(
            lambda x: x.strftime("%Y-%m-%d %H:%M:%S.%f")
        )
        print(result.shape[0])
        result = result.to_dict(orient="records")
    except SQLAlchemyError as e:
        logger.error(f"Database error in bbps_data_entry(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in bbps_data_entry(): {e}")
    return {"matched_bbps_data": result}
