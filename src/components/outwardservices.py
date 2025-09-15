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


def get_ebo_wallet_data(start_date, end_date):
    logger.info("Fetching Data from EBO Wallet Transaction")
    ebo_df = None
    query = text(
        """
        SELECT  
        mt2.TransactionRefNum AS IHubReferenceId,
        ewt.MasterTransactionsId,
        MAX(
            CASE 
                WHEN ewt.Description IN ('Transaction - Credit', 'Transaction - Credit due to failure', 'Transaction - Refund','Manual Refund Credit - Transaction - Credit') 
                OR LOWER(ewt.Description) LIKE "%refund credit%" 
                THEN 'Yes' 
                ELSE 'No' 
            END
        ) AS TRANSACTION_CREDIT,
        MAX(CASE WHEN ewt.Description IN ('Transaction - Debit','Manual Refund Debit - Transaction - Debit') THEN 'Yes' ELSE 'No' END) AS TRANSACTION_DEBIT,
        MAX(CASE WHEN ewt.Description IN ('Commission Added','Manual Refund Credit - Commission - Added') THEN 'Yes' ELSE 'No' END) AS COMMISSION_CREDIT,
        MAX(CASE WHEN ewt.Description IN ('Commission - Reversal','Commission Reversal','Manual Refund Debit - Commission - Reversal') THEN 'Yes' ELSE 'No' END) AS COMMISSION_REVERSAL
        FROM
        ihubcore.MasterTransaction mt2
        JOIN  
        tenantinetcsc.EboWalletTransaction ewt
        ON mt2.TenantMasterTransactionId = ewt.MasterTransactionsId
        WHERE
        DATE(mt2.CreationTs) BETWEEN :start_date AND :end_date
        GROUP BY
        mt2.TransactionRefNum,
        ewt.MasterTransactionsId
    """
    )

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


# Recharge service function ---------------------------------------------------
def recharge_Service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        f"""
        SELECT mt2.TransactionRefNum AS IHUB_REFERENCE,
               mt2.TenantDetailId as TENANT_ID,   
               sn.requestID AS VENDOR_REFERENCE,
               mt2.CreationUserId as IHUB_USERNAME,
               mt2.TransactionStatus AS IHUB_MASTER_STATUS,
               mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
               sn.CreationTs AS SERVICE_DATE, 
               sn.rechargeStatus AS service_status,
               sn.Amount as RECHARGE_AMOUNT,
               CASE
                   WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS IHUB_LEDGER_STATUS,
               CASE
                   WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS TENANT_LEDGER_STATUS
        FROM ihubcore.MasterTransaction mt2
        LEFT JOIN ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN ihubcore.PsRechargeTransaction sn ON sn.MasterSubTransactionId = mst.Id
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) iwt ON iwt.IHubReferenceId = mt2.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) twt ON twt.IHubReferenceId = mt2.TransactionRefNum
        WHERE DATE(sn.CreationTs) BETWEEN :start_date AND :end_date
    """
    )
    params = {"start_date": start_date, "end_date": end_date}
    try:
        df_db = execute_sql_with_retry(query, params=params)
        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()
        status_mapping = {
            0: "initiated",
            1: "success",
            2: "pending",
            3: "failed",
            4: "instant failed",
        }
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        df_db = map_tenant_id_column(df_db)
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)
    except SQLAlchemyError as e:
        logger.error(f"Database error in recharge_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in recharge_Service(): {e}")
    return result


# ---------------------------------------------------------------------------------------
# BBPS FUNCTION
def Bbps_service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()

    # Step 1: Load main transaction data
    core_query = text(
        f"""
    SELECT
        mt2.TransactionRefNum as IHUB_REFERENCE,
        mt2.CreationUserId as IHUB_USERNAME,
        bbp.TxnRefId as VENDOR_REFERENCE,
        bbp.Amount as AMOUNT,
        bbp.creationTs as SERVICE_DATE,
        mt2.TransactionStatus AS IHUB_MASTER_STATUS,
        mt2.tenantDetailID as TENANT_ID,
        mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
        bbp.TransactionStatusType as service_status,
        bbp.HeadReferenceId
    FROM ihubcore.MasterTransaction mt2
    LEFT JOIN ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt2.Id
    LEFT JOIN ihubcore.BBPS_BillPay bbp ON bbp.MasterSubTransactionId = mst.Id
    WHERE DATE(bbp.CreationTs) BETWEEN :start_date AND :end_date
    """
    )
    params = {"start_date": start_date, "end_date": end_date}

    core_df = execute_sql_with_retry(core_query, params=params)

    # Step 2: Load flags
    query = text(
        f"""
        SELECT DISTINCT IHubReferenceId FROM ihubcore.IHubWalletTransaction WHERE  DATE(creationTs) BETWEEN :start_date AND :end_date
    """
    )
    ihub_refs = execute_sql_with_retry(query, params=params)

    # Step 2: Load flags
    query = text(
        f"""
        SELECT DISTINCT id FROM ihubcore.BBPS_BillFetch WHERE  DATE(creationTs) BETWEEN :start_date AND :end_date
    """
    )
    bbps_fetch_ids = execute_sql_with_retry(query, params=params)

    query = text(
        f"""
        SELECT DISTINCT IHubReferenceId FROM ihubcore.TenantWalletTransaction WHERE  DATE(CreationTs) BETWEEN :start_date AND :end_date
    """
    )
    tenant_refs = execute_sql_with_retry(query, params=params)

    # Step 3: Add flags to core_df
    core_df["IHUB_LEDGER_STATUS"] = (
        core_df["IHUB_REFERENCE"]
        .isin(ihub_refs["IHubReferenceId"])
        .map({True: "Yes", False: "No"})
    )
    core_df["BILL_FETCH_STATUS"] = (
        core_df["HeadReferenceId"]
        .isin(bbps_fetch_ids["id"])
        .map({True: "Yes", False: "No"})
    )
    core_df["TENANT_LEDGER_STATUS"] = (
        core_df["IHUB_REFERENCE"]
        .isin(tenant_refs["IHubReferenceId"])
        .map({True: "Yes", False: "No"})
    )

    # Final result
    print(core_df.shape[0])

    params = {"start_date": start_date, "end_date": end_date}
    try:
        # df_db = execute_sql_with_retry(query, params=params)
        if core_df.empty:
            logger.warning(f"No data returned for service:{service_name}")
            return pd.DataFrame()
        status_mapping = {
            0: "unknown",
            1: "success",
            2: "failed",
            3: "inprogress",
            4: "partialsuccuess",
        }
        core_df = map_status_column(
            core_df,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        core_df = map_tenant_id_column(core_df)
        result = merge_ebo_wallet_data(
            core_df, start_date, end_date, get_ebo_wallet_data
        )
    except SQLAlchemyError as e:
        logger.error(f"Databasr error in BBPS_SERVICE():{e}")
    except Exception as e:
        logger.error(f"Unexpected error in BBPS_SERVICE():{e} ")
    return result


# ------------------------------------------------------------------------
# PAN-UTI Service function
def Panuti_service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        f"""
       SELECT 
               mt2.TransactionRefNum AS IHUB_REFERENCE,
               mt2.TenantDetailId as TENANT_ID,   
               u2.ApplicationNumber  AS VENDOR_REFERENCE,
               mt2.CreationUserId as IHUB_USERNAME,
               mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
               mt2.TransactionStatus AS IHUB_MASTER_STATUS,
               u2.CreationTs  AS SERVICE_DATE, 
               u2.TransactionStatusType AS service_status,
               u2.TransactionAmount  as AMOUNT,
               CASE
                   WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS IHUB_LEDGER_STATUS,
               CASE
                   WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS TENANT_LEDGER_STATUS
        FROM ihubcore.UTIITSLTTransaction u2  
        LEFT JOIN ihubcore.MasterSubTransaction mst ON u2.MasterSubTransactionId = mst.Id 
        LEFT JOIN ihubcore.MasterTransaction mt2 ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date and :end_date
        ) iwt ON iwt.IHubReferenceId = mt2.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date and :end_date
        ) twt ON twt.IHubReferenceId = mt2.TransactionRefNum
        WHERE DATE(u2.CreationTs) BETWEEN :start_date and :end_date
        """
    )
    params = {"start_date": start_date, "end_date": end_date}
    try:
        df_db = execute_sql_with_retry(query, params=params)
        if df_db.empty:
            logger.warning(f"No data returned for service:{service_name}")
            return pd.DataFrame()
        status_mapping = {
            0: "failed",
            1: "success",
        }
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        df_db = map_tenant_id_column(df_db)
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)
    except SQLAlchemyError as e:
        logger.error(f"Databasr error in PAN_UTI_SERVICE():{e}")
    except Exception as e:
        logger.error(f"Unexpected error in PAN_UTI_SERVICE():{e} ")
    return result


# ----------------------------------------------------------------------------------------
# DMT SERVICE FUNCTION-------------------------------------------------------------------
def dmt_Service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = text(
        f"""
            SELECT mt2.TransactionRefNum AS IHUB_REFERENCE,
            pst.VendorReferenceId as VENDOR_REFERENCE,
            mt2.CreationUserId as IHUB_USERNAME,
            mt2.TransactionStatus AS IHUB_MASTER_STATUS,
            mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
            pst.PaySprintTransStatus as service_status,
            CASE
            WHEN a.IHubReferenceId  IS NOT NULL THEN 'Yes'
            ELSE 'No'
            END AS IHUB_LEDGER_STATUS,
            CASE
                   WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS TENANT_LEDGER_STATUS
            FROM
            ihubcore.MasterTransaction mt2
            LEFT JOIN
            ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt2.Id
            LEFT JOIN
            ihubcore.PaySprint_Transaction pst ON pst.MasterSubTransactionId = mst.Id
            LEFT JOIN
            (SELECT DISTINCT iwt.IHubReferenceId AS IHubReferenceId
            FROM ihubcore.IHubWalletTransaction iwt
            WHERE DATE(iwt.CreationTs) BETWEEN :start_date and :end_date
            ) a ON a.IHubReferenceId = mt2.TransactionRefNum
            LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date and :end_date
            ) twt ON twt.IHubReferenceId = mt2.TransactionRefNum
            WHERE
            DATE(pst.CreationTs) BETWEEN :start_date and :end_date 
            """
    )
    params = {"start_date": start_date, "end_date": end_date}
    try:
        df_db = execute_sql_with_retry(query, params=params)
        df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
        if df_db.empty:
            logger.warning(f"No data returned for service:{service_name}")
            return pd.DataFrame()
        status_mapping = {
            0: "failedandrefunded",
            1: "success",
            2: "inprocess",
            3: "sendtobank",
            4: "onhold",
            5: "failed",
            111: "tenantwalletinit",
        }
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        df_db = map_tenant_id_column(df_db)

        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)
    except SQLAlchemyError as e:
        logger.error(f"Databasr error in DMT_SERVICE():{e}")
    except Exception as e:
        logger.error(f"Unexpected error in DMT_SERVICE:{e} ")
    return result


# ------------------------------------------------------------------------
# PAN-NSDL Service function
def Pannsdl_service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        f"""
       SELECT  mt2.TransactionRefNum AS IHUB_REFERENCE,
               mt2.TenantDetailId as TENANT_ID,   
               pit.AcknowledgeNo  AS VENDOR_REFERENCE,
               mt2.CreationUserId as IHUB_USERNAME,
               mt2.TransactionStatus AS IHUB_MASTER_STATUS,
               mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
               DATE(pit.CreationTs)  AS SERVICE_DATE, 
               pit.ApplicationStatus AS service_status,
               pit.Amount as AMOUNT,
               CASE
                   WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS IHUB_LEDGER_STATUS,
               CASE
                   WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS TENANT_LEDGER_STATUS
        FROM ihubcore.PanInTransaction pit  
        LEFT JOIN ihubcore.MasterSubTransaction mst ON pit.MasterSubTransactionId = mst.Id 
        LEFT JOIN ihubcore.MasterTransaction mt2 ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date and :end_date
        ) iwt ON iwt.IHubReferenceId = mt2.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date and :end_date
        ) twt ON twt.IHubReferenceId = mt2.TransactionRefNum
        WHERE DATE(pit.ApplicationStatusTs) BETWEEN :start_date and :end_date and pit.AcknowledgeNo  IS NOT NULL
        """
    )
    params = {"start_date": start_date, "end_date": end_date}
    try:
        df_db = execute_sql_with_retry(query, params=params)
        if df_db.empty:
            logger.warning(f"No data returned for service:{service_name}")
            return pd.DataFrame()
        status_mapping = {
            0: "None",
            1: "New",
            2: "Acknowledged",
            3: "Rejected",
            4: "Uploaded",
            5: "Processed",
            6: "Reupload",
            7: "Alloted",
            8: "Objection",
            9: "MoveToNew",
        }
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        df_db = map_tenant_id_column(df_db)
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)
    except SQLAlchemyError as e:
        logger.error(f"Databasr error in PAN_NSDL_SERVICE():{e}")
    except Exception as e:
        logger.error(f"Unexpected error in PAN_NSDL_SERVICE():{e} ")
    return result


# ------------------------------------------------------------------------------
# passport service function
def passport_service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        f"""
        SELECT mt2.TransactionRefNum AS IHUB_REFERENCE,
               mt2.TenantDetailId as TENANT_ID,   
               pi.BankReferenceNumber  AS VENDOR_REFERENCE,
               mt2.CreationUserId as IHUB_USERNAME,
               mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
               mt2.TransactionStatus AS IHUB_MASTER_STATUS,
               pi.BankReferenceTs AS SERVICE_DATE, 
               pi.PassportInStatusType AS service_status,
               pi.Amount as HUB_AMOUNT,
               CASE
                   WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS IHUB_LEDGER_STATUS,
               CASE
                   WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS TENANT_LEDGER_STATUS
        FROM ihubcore.PassportIn pi 
        LEFT JOIN ihubcore.MasterSubTransaction mst ON pi.MasterSubTransactionId = mst.Id 
        LEFT JOIN ihubcore.MasterTransaction mt2 ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) iwt ON iwt.IHubReferenceId = mt2.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) twt ON twt.IHubReferenceId = mt2.TransactionRefNum
        WHERE DATE(pi.BankReferenceTs) BETWEEN :start_date AND :end_date      
    """
    )
    params = {"start_date": start_date, "end_date": end_date}
    try:
        df_db = execute_sql_with_retry(query, params=params)
        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()
        status_mapping = {
            0: "unknown",
            1: "NewRequest",
            2: "CallMade",
            3: "AppointmentFixed",
            4: "ApplicationClosed",
            5: "ApplicationRejected",
            6: "ReAppointment",
        }
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        df_db = map_tenant_id_column(df_db)
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)
    except SQLAlchemyError as e:
        logger.error(f"Database error in Passport_service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in Passport_service(): {e}")
    return result


# ------------------------------------------------------------------------
# LIC PREMIMUM FUNCTION
def lic_service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        f"""
        SELECT mt2.TransactionRefNum AS IHUB_REFERENCE,
               mt2.TenantDetailId as TENANT_ID,   
               lpt.OrderId AS VENDOR_REFERENCE,
               mt2.CreationUserId as IHUB_USERNAME,
               mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT, 
               mt2.TransactionStatus AS IHUB_MASTER_STATUS,
               lpt.CreationTs AS SERVICE_DATE,
               lpf.Billedamount as LIC_AMOUNT, 
               lpt.BillPayStatus AS service_status,
               CASE
                   WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS IHUB_LEDGER_STATUS,
               CASE
                   WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS TENANT_LEDGER_STATUS
        FROM ihubcore.LicPremiumTransaction lpt
        LEFT JOIN ihubcore.MasterSubTransaction mst ON lpt.MasterSubTransactionId = mst.Id
        LEFT JOIN ihubcore.MasterTransaction mt2 ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN ihubcore.LicPremiumBillFetch lpf ON lpf.id = lpt.BillFetchId  
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) iwt ON iwt.IHubReferenceId = mt2.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) twt ON twt.IHubReferenceId = mt2.TransactionRefNum
        WHERE DATE(lpt.CreationTs) BETWEEN :start_date AND :end_date      
    """
    )
    params = {"start_date": start_date, "end_date": end_date}

    try:

        # Execute with retry logic
        df_db = execute_sql_with_retry(
            query,
            params=params,
        )
        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()
        # Status mapping with fallback
        status_mapping = {
            0: "initiated",
            1: "success",
            2: "failed",
        }
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        df_db = map_tenant_id_column(df_db)
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)
    except SQLAlchemyError as e:
        logger.error(f"Database error in recharge_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in recharge_Service(): {e}")

    return result


# ----------------------------------------------------------------------
# Astro service Function
def astro_service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        f"""
        SELECT mt2.TransactionRefNum AS IHUB_REFERENCE,
               mt2.TenantDetailId as TENANT_ID,   
               at2.OrderId AS VENDOR_REFERENCE,
               mt2.CreationUserId as IHUB_USERNAME,
                mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT, 
               mt2.TransactionStatus AS IHUB_MASTER_STATUS,
               at2.CreationTs AS SERVICE_DATE, 
               at2.AstroTransactionStatus AS service_status,
               CASE
                   WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS IHUB_LEDGER_STATUS,
               CASE
                   WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS TENANT_LEDGER_STATUS
        FROM ihubcore.MasterTransaction mt2
        LEFT JOIN ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN ihubcore.AstroTransaction at2 ON at2.MasterSubTransactionId = mst.Id
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) iwt ON iwt.IHubReferenceId = mt2.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) twt ON twt.IHubReferenceId = mt2.TransactionRefNum
        WHERE DATE(at2.CreationTs) BETWEEN :start_date AND :end_date      
    """
    )
    params = {"start_date": start_date, "end_date": end_date}

    try:

        df_db = execute_sql_with_retry(query, params=params)
        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()
        # Status mapping with fallback
        status_mapping = {
            0: "initiated",
            1: "unknown",
            2: "success",
        }
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        df_db = map_tenant_id_column(df_db)
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)

    except SQLAlchemyError as e:
        logger.error(f"Database error in ASTRO_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in ASTRO_Service(): {e}")
    return result


# ----------------------------------------------------------------------------------
# Insurance Offline Function
def insurance_offline_service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        f""" 
        SELECT 
        mt.TransactionRefNum AS IHUB_REFERENCE,
        niit.PolicyNumber  AS VENDOR_REFERENCE,
        mt.CreationUserId as IHUB_USERNAME,
        mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
        mt.TransactionStatus AS IHUB_MASTER_STATUS,
        mt.TenantDetailId as TENANT_ID,   
        niit.CreationTs AS SERVICE_DATE,
        niit.InsuranceStatusType AS service_status,
        niit.Amount as HUB_AMOUNT,
        CASE 
            WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
            ELSE 'No'
        END AS IHUB_LEDGER_STATUS,
        CASE
            WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
            ELSE 'No'
        END AS TENANT_LEDGER_STATUS
        FROM 
        ihubcore.MasterTransaction mt
        LEFT JOIN ihubcore.MasterSubTransaction mst
            ON  mst.MasterTransactionId =  mt.id 
        LEFT JOIN  ihubcore.NewIndiaInsuranceTransaction niit  
            ON mst.Id = niit.MasterSubTransactionId 
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) iwt ON iwt.IHubReferenceId = mt.TransactionRefNum
        LEFT JOIN (
                    SELECT DISTINCT IHubReferenceId
                    FROM ihubcore.TenantWalletTransaction
                    WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
                ) twt ON twt.IHubReferenceId = mt.TransactionRefNum
        WHERE 
        DATE(niit.CreationTs) BETWEEN :start_date AND :end_date 
    """
    )
    params = {"start_date": start_date, "end_date": end_date}

    try:

        df_db = execute_sql_with_retry(query, params=params)
        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()
        # Status mapping with fallback
        status_mapping = {
            0: "unknown",
            1: "NewRequest",
            2: "InProcess",
            3: "Completed",
            4: "Rejected",
        }
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        df_db = map_tenant_id_column(df_db)
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)
    except SQLAlchemyError as e:
        logger.error(f"Database error in insurance_offline_service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in insurance_offline_service(): {e}")
    return result


# --------------------------------------------------------------------------------------------------------
def abhibus_service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        """
        SELECT 
            mt.TransactionRefNum AS IHUB_REFERENCE,
            abt.PnrNumber AS VENDOR_REFERENCE,
            mt.CreationUserId as IHUB_USERNAME,
            mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,    
            mt.TransactionStatus AS IHUB_MASTER_STATUS,
            mt.TenantDetailId AS TENANT_ID,
            abt.CreationTs AS SERVICE_DATE,
            abt.TicketStatusType  AS service_status,
            abt.TotalAmount  AS AMOUNT,
            CASE 
                WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
                ELSE 'No'
            END AS IHUB_LEDGER_STATUS,
            CASE
                WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                ELSE 'No'
            END AS TENANT_LEDGER_STATUS
        FROM
            ihubcore.MasterTransaction mt
        LEFT JOIN ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt.id
        LEFT JOIN ihubcore.AbhiBus_TicketDetail abt ON mst.Id = abt.MasterSubTransactionId
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date 
        ) iwt ON iwt.IHubReferenceId = mt.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date 
        ) twt ON twt.IHubReferenceId = mt.TransactionRefNum
        WHERE 
            DATE(abt.CreationTs) BETWEEN :start_date AND :end_date  
                 
"""
    )
    params = {"start_date": start_date, "end_date": end_date}
    try:
        df_db = execute_sql_with_retry(query, params=params)
        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()
        # Status mapping with fallback
        status_mapping = {
            0: "initated",
            2: "success",
            3: "failed",
            4: "failed and refunded",
        }
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        df_db = map_tenant_id_column(df_db)
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)
    except SQLAlchemyError as e:
        logger.error(f"Database error in abhibus_service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in abhibus_service(): {e}")
    return result


def moveToBank_service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        """
        SELECT mt.TransactionRefNum AS IHUB_REFERENCE,
               mt.CreationUserId as IHUB_USERNAME,
               mt.TenantDetailId AS TENANT_ID,
               mt.TransactionStatus AS IHUB_MASTER_STATUS,
               mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
               mt.CreationTs AS SERVICE_DATE,
               amt.TxnAmount AS AMOUNT,amt.TransactionStatus as service_status ,amt.requestUUID as VENDOR_REFERENCE,
               CASE
                   WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS IHUB_LEDGER_STATUS,
               CASE
                   WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS TENANT_LEDGER_STATUS
        FROM ihubcore.MasterTransaction mt
        LEFT JOIN ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt.id
        left join ihubcore.AxisMtbTransaction amt on amt.MasterSubTransactionId = mst.Id 
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) iwt ON iwt.IHubReferenceId = mt.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) twt ON twt.IHubReferenceId = mt.TransactionRefNum
        WHERE DATE(amt.CreationTs) BETWEEN :start_date AND :end_date 
    """
    )
    params = {"start_date": start_date, "end_date": end_date}
    try:
        df_db = execute_sql_with_retry(query, params=params)
        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()
        status_mapping = {
            0: "unknown",
            1: "pending",
            2: "failed",
            3: "success",
            4: "failed and refunded",
        }
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        df_db = map_tenant_id_column(df_db)
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)
    except SQLAlchemyError as e:
        logger.error(f"Database error in moveToBank_service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in moveToBank_service(): {e}")
    return result


# ------------------------------------------------------------------------


def manualTB_sevice(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        """
        SELECT mt.TransactionRefNum AS IHUB_REFERENCE,
               mt.CreationUserId as IHUB_USERNAME,
               mt.TenantDetailId AS TENANT_ID,
               mt.TransactionStatus AS IHUB_MASTER_STATUS,
               mst.NetCommissionAddedToEBOWallet AS COMMISSION_AMOUNT,
               mt.CreationTs AS SERVICE_DATE,
               amt.TxnAmount AS AMOUNT,amt.TransactionStatus as service_status ,amt.requestUUID as VENDOR_REFERENCE,
               CASE
                   WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS IHUB_LEDGER_STATUS,
               CASE
                   WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS TENANT_LEDGER_STATUS
        FROM ihubcore.MasterTransaction mt
        LEFT JOIN ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt.id
        left join ihubcore.AxisMtbTransaction amt on amt.MasterSubTransactionId = mst.Id 
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) iwt ON iwt.IHubReferenceId = mt.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
        ) twt ON twt.IHubReferenceId = mt.TransactionRefNum
        WHERE DATE(amt.CreationTs) BETWEEN :start_date AND :end_date 
    """
    )
    params = {"start_date": start_date, "end_date": end_date}
    try:
        df_db = execute_sql_with_retry(query, params=params)
        if df_db.empty:
            logger.warning(f"No data returned for service: {service_name}")
            return pd.DataFrame()
        status_mapping = {
            0: "unknown",
            1: "New Request",
            2: "Yet To Credit",
            3: "Approved by Accountant",
            4: "Approved by Accountant Special",
            5: "Approved by Admin",
            6: "Approved by Admin Special",
            7: "Cancel",
        }
        df_db = map_status_column(
            df_db,
            "service_status",
            status_mapping,
            new_column=f"{service_name}_STATUS",
            drop_original=True,
        )
        df_db = map_tenant_id_column(df_db)
        result = merge_ebo_wallet_data(df_db, start_date, end_date, get_ebo_wallet_data)
    except SQLAlchemyError as e:
        logger.error(f"Database error in manualTB_service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in manualTB_service(): {e}")
    return result


# ------------------------------------------------------------------------

# tenant database filtering function------------------------------------------------
# def tenant_filtering(start_date, end_date, tenant_service_id, hub_service_id):
#     logger.info("Entered Tenant filtering function")
#     result = None

#     # Prepare a safe, parameterized query
#     query = text(
#         """
#         WITH cte AS (
#             SELECT src.Id as TENANT_ID,
#                    src.UserName as IHUB_USERNAME,
#                    src.TranAmountTotal as AMOUNT,
#                    src.TransactionStatus as TENANT_STATUS,
#                    src.CreationTs as SERVICE_DATE,
#                    src.VendorSubServiceMappingId,
#                    hub.Id AS hub_id
#             FROM (
#                 SELECT mt.*, u.UserName
#                 FROM tenantinetcsc.MasterTransaction mt
#                 LEFT JOIN tenantinetcsc.EboDetail ed ON ed.id = mt.EboDetailId
#                 LEFT JOIN tenantinetcsc.`User` u ON u.Id = ed.UserId
#                 WHERE DATE(mt.CreationTs) BETWEEN :start_date AND :end_date
#                 AND mt.VendorSubServiceMappingId IN :tenant_service_id

#                 UNION ALL

#                 SELECT umt.*, u.UserName
#                 FROM tenantupcb.MasterTransaction umt
#                 LEFT JOIN tenantupcb.EboDetail ed ON ed.id = umt.EboDetailId
#                 LEFT JOIN tenantupcb.`User` u ON u.Id = ed.UserId
#                 WHERE DATE(umt.CreationTs) BETWEEN :start_date AND :end_date
#                 AND umt.VendorSubServiceMappingId IN :tenant_service_id

#                 UNION ALL

#                 SELECT imt.*, u.UserName
#                 FROM tenantiticsc.MasterTransaction imt
#                 LEFT JOIN tenantiticsc.EboDetail ed ON ed.id = imt.EboDetailId
#                 LEFT JOIN tenantiticsc.`User` u ON u.Id = ed.UserId
#                 WHERE DATE(imt.CreationTs) BETWEEN :start_date AND :end_date
#                 AND imt.VendorSubServiceMappingId IN :tenant_service_id
#             ) AS src
#             LEFT JOIN ihubcore.MasterTransaction AS hub
#             ON hub.TenantMasterTransactionId = src.Id
#             AND DATE(hub.CreationTs) BETWEEN :start_date AND :end_date
#             AND hub.VendorSubServiceMappingId IN :hub_service_id
#         )
#         SELECT *
#         FROM cte
#         WHERE hub_id IS NULL
#     """
#     )
#     # print(query)
#     # print(tenant_service_id, hub_service_id)

#     tenant_service_id = (
#         [tenant_service_id] if isinstance(tenant_service_id, int) else tenant_service_id
#     )
#     hub_service_id = (
#         [x for x in hub_service_id]
#         if isinstance(hub_service_id, (tuple, list))
#         else [hub_service_id]
#     )
#     # Convert lists to tuples for SQLAlchemy to treat them correctly in IN clauses
#     params = {
#         "start_date": start_date,
#         "end_date": end_date,
#         "tenant_service_id": tuple(tenant_service_id),
#         "hub_service_id": tuple(hub_service_id),
#     }
#     # print(params)
#     try:
#         result = execute_sql_with_retry(query, params=params)
#     except SQLAlchemyError as e:
#         logger.error(f"Database error in Tenant DB Filtering: {e}")
#     except Exception as e:
#         logger.error(f"Unexpected error in Tenant DB Filtering: {e}")

#     return result


# -----------------------
