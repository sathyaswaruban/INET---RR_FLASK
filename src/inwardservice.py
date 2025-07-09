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
                df_cleaned["VENDOR_STATUS"] = df_cleaned["VENDOR_STATUS"].replace(
                    {"Transaction Successful.": "success"}
                ).fillna("failed")
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


def filtering_Data(df_db, df_excel, service_name):
    try:
        logger.info(f"Filteration Starts for {service_name} service")
        mapping = None
        Excel_count = len(df_excel)
        Hub_count = df_db.shape[0]
        # converting the date of both db and excel to string
        df_db["SERVICE_DATE"] = df_db["SERVICE_DATE"].dt.strftime("%Y-%m-%d")
        # tenant_data["SERVICE_DATE"] = tenant_data["SERVICE_DATE"].dt.strftime("%Y-%m-%d")

        df_excel["VENDOR_DATE"] = pd.to_datetime(
            df_excel["VENDOR_DATE"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
            # Mapping names with corresponding values
        status_mapping = {
            0: "initiated",
            1: "success",
            2: "failed",
            3: "inprogress",
            4: "partial success",
        }
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
        # 1 Filtering Data initiated in IHUB portal and not in Vendor Xl
        not_in_vendor = df_db[~df_db["VENDOR_REFERENCE"].isin(df_excel["REFID"])].copy()
        not_in_vendor["CATEGORY"] = "NOT_IN_VENDOR"
        if service_name == 'AEPS':
            not_in_vendor = not_in_vendor.rename(columns={"AEPS_AMOUNT": "AMOUNT"})
        elif service_name == 'MATM':
            not_in_vendor = not_in_vendor.rename(columns={"MATM_AMOUNT": "AMOUNT"})
        not_in_vendor = safe_column_select(not_in_vendor, required_columns)
        # 2. Filtering Data Present in Vendor XL but Not in Ihub Portal
        not_in_portal = df_excel[
            ~df_excel["REFID"].isin(df_db["VENDOR_REFERENCE"])
        ].copy()
        not_in_portal["CATEGORY"] = "NOT_IN_PORTAL"
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

        success_count = matched_success_status.shape[0]
        matched_failed_status = matched[
            (matched[f"{service_name}_STATUS"].str.lower() == "failed")
            & (matched["VENDOR_STATUS"].str.lower() == "failed")
        ]
        failed_count = matched_failed_status.shape[0]
        # Scearios Blocks Based on Not In Ledger (NIL) and In Ledger ```````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````
        # SCENARIO 1 VEND_IHUB_SUC-NIL
        vend_ihub_succ_not_in_ledger = matched[
            (matched["VENDOR_STATUS"].str.lower() == "success")
            & (matched["IHUB_MASTER_STATUS"].str.lower() == "success")
            & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
        ].copy()
        vend_ihub_succ_not_in_ledger["CATEGORY"] = "VEND_IHUB_SUC-NIL"
        vend_ihub_succ_not_in_ledger = safe_column_select(
            vend_ihub_succ_not_in_ledger, required_columns
        )
        # SCENARIO 2 VEND_FAIL_IHUB_SUC-NIL
        vend_fail_ihub_succ_not_in_ledger = matched[
            (matched["VENDOR_STATUS"].str.lower() == "failed")
            & (matched["IHUB_MASTER_STATUS"].str.lower() == "success")
            & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
        ].copy()
        vend_fail_ihub_succ_not_in_ledger["CATEGORY"] = "VEND_FAIL_IHUB_SUC-NIL"
        vend_fail_ihub_succ_not_in_ledger = safe_column_select(
            vend_fail_ihub_succ_not_in_ledger, required_columns
        )
        # SCENARIO 3 VEND_SUC_IHUB_FAIL-NIL
        vend_succ_ihub_fail_not_in_ledger = matched[
            (matched["VENDOR_STATUS"].str.lower() == "success")
            & (matched["IHUB_MASTER_STATUS"].str.lower() == "failed")
            & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
        ].copy()
        vend_succ_ihub_fail_not_in_ledger["CATEGORY"] = "VEND_SUC_IHUB_FAIL-NIL"
        vend_succ_ihub_fail_not_in_ledger = safe_column_select(
            vend_succ_ihub_fail_not_in_ledger, required_columns
        )
        # SCENARIO 5 IHUB_INT_VEND_SUC-NIL
        ihub_initiate_vend_succes_not_in_ledger = matched[
            (matched["VENDOR_STATUS"].str.lower() == "success")
            & (
                matched["IHUB_MASTER_STATUS"]
                .str.lower()
                .isin(["initiated", "inprogress"])
            )
            & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
        ].copy()
        ihub_initiate_vend_succes_not_in_ledger["CATEGORY"] = "IHUB_INT_VEND_SUC-NIL"
        ihub_initiate_vend_succes_not_in_ledger = safe_column_select(
            ihub_initiate_vend_succes_not_in_ledger, required_columns
        )
        # SCENARIO 6 VEND_FAIL_IHUB_INT-NIL
        ihub_initiate_vend_fail_not_in_ledger = matched[
            (matched["VENDOR_STATUS"].str.lower() == "failed")
            & (
                matched["IHUB_MASTER_STATUS"]
                .str.lower()
                .isin(["initiated", "inprogress"])
            )
            & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
        ].copy()
        ihub_initiate_vend_fail_not_in_ledger["CATEGORY"] = " VEND_FAIL_IHUB_INT-NIL"
        ihub_initiate_vend_fail_not_in_ledger = safe_column_select(
            ihub_initiate_vend_fail_not_in_ledger, required_columns
        )

        # SCENARIO 2 VEND_FAIL_IHUB_SUC IL
        vend_fail_ihub_succ = matched[
            (matched["VENDOR_STATUS"].str.lower() == "failed")
            & (matched["IHUB_MASTER_STATUS"].str.lower() == "success")
            & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
        ].copy()
        vend_fail_ihub_succ["CATEGORY"] = "VEND_FAIL_IHUB_SUC"
        vend_fail_ihub_succ = safe_column_select(vend_fail_ihub_succ, required_columns)
        # SCENARIO 3 VEND_SUC_IHUB_FAIL
        vend_succ_ihub_fail = matched[
            (matched["VENDOR_STATUS"].str.lower() == "success")
            & (matched["IHUB_MASTER_STATUS"].str.lower() == "failed")
            & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
        ].copy()
        vend_succ_ihub_fail["CATEGORY"] = "VEND_SUC_IHUB_FAIL"
        vend_succ_ihub_fail = safe_column_select(vend_succ_ihub_fail, required_columns)
        # SCENARIO 4 IHUB_VEND_FAIL IL
        ihub_vend_fail = matched[
            (matched["VENDOR_STATUS"].str.lower() == "failed")
            & (matched["IHUB_MASTER_STATUS"].str.lower() == "failed")
            & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
        ].copy()
        ihub_vend_fail["CATEGORY"] = "IHUB_VEND_FAIL"
        ihub_vend_fail = safe_column_select(ihub_vend_fail, required_columns)
        # SCENARIO 5 IHUB_INT_VEND_SUC IL
        ihub_initiate_vend_succes = matched[
            (matched["VENDOR_STATUS"].str.lower() == "success")
            & (
                matched["IHUB_MASTER_STATUS"]
                .str.lower()
                .isin(["initiated", "inprogress"])
            )
            & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
        ].copy()
        ihub_initiate_vend_succes["CATEGORY"] = "IHUB_INT_VEND_SUC"
        ihub_initiate_vend_succes = safe_column_select(
            ihub_initiate_vend_succes, required_columns
        )

        # SCENARIO 6 VEND_FAIL_IHUB_INT IL
        ihub_initiate_vend_fail = matched[
            (matched["VENDOR_STATUS"].str.lower() == "failed")
            & (
                matched["IHUB_MASTER_STATUS"]
                .str.lower()
                .isin(["initiated", "inprogress"])
            )
            & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
        ].copy()
        ihub_initiate_vend_fail["CATEGORY"] = "VEND_FAIL_IHUB_INT"
        ihub_initiate_vend_fail = safe_column_select(
            ihub_initiate_vend_fail, required_columns
        )
        # Scenario Block ends-----------------------------------------------------------------------------------
        # tenant_data["CATEGORY"] = "TENANT_DB_INTI - NOT_IN_IHUB"
        # Combining all Scenarios
        combined = [
            not_in_vendor,
            not_in_portal,
            # not_in_portal_vendor_success,
            # mismatched,
            # tenant_data,
            vend_ihub_succ_not_in_ledger,
            vend_fail_ihub_succ_not_in_ledger,
            vend_succ_ihub_fail_not_in_ledger,
            # ihub_vend_fail_not_in_ledger,
            ihub_initiate_vend_succes_not_in_ledger,
            ihub_initiate_vend_fail_not_in_ledger,
            # vend_ihub_succ,
            vend_fail_ihub_succ,
            vend_succ_ihub_fail,
            # ihub_vend_fail,
            ihub_initiate_vend_succes,
            ihub_initiate_vend_fail,
        ]
        all_columns = set().union(*[df.columns for df in combined])
        aligned_dfs = []
        for df in combined:
            # create missing columns with None
            df_copy = df.copy()  # 🛡️ Make a copy so original is not modified
            for col in all_columns - set(df_copy.columns):
                df_copy[col] = None
            df_copy = df_copy[list(all_columns)]  # Reorder columns
            aligned_dfs.append(df_copy)
        # Filter out DataFrames that are completely empty or contain only NA values
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
            combined = pd.concat(non_empty_dfs, ignore_index=True)
            logger.info("Filteration Ends")

            # Mapping all Scenarios with keys as Dictionary to retrun as result
            mapping = {
                "not_in_vendor": not_in_vendor,
                "combined": combined,
                "not_in_Portal": not_in_portal,
                # "mismatched": mismatched,
                # "NOT_IN_PORTAL_VENDOR_SUCC": not_in_portal_vendor_success,
                # "Tenant_db_ini_not_in_hubdb": tenant_data,
                "VEND_IHUB_SUC-NIL": vend_ihub_succ_not_in_ledger,
                "VEND_FAIL_IHUB_SUC-NIL": vend_fail_ihub_succ_not_in_ledger,
                "VEND_SUC_IHUB_FAIL-NIL": vend_succ_ihub_fail_not_in_ledger,
                # "IHUB_VEND_FAIL-NIL": ihub_vend_fail_not_in_ledger,
                "IHUB_INT_VEND_SUC-NIL": ihub_initiate_vend_succes_not_in_ledger,
                "VEND_FAIL_IHUB_INT-NIL": ihub_initiate_vend_fail_not_in_ledger,
                # "VEND_IHUB_SUC": vend_ihub_succ,
                "VEND_FAIL_IHUB_SUC": vend_fail_ihub_succ,
                "VEND_SUC_IHUB_FAIL": vend_succ_ihub_fail,
                # "IHUB_VEND_FAIL": ihub_vend_fail,
                "IHUB_INT_VEND_SUC": ihub_initiate_vend_succes,
                "VEND_FAIL_IHUB_INT": ihub_initiate_vend_fail,
                "Total_Success_count": success_count,
                "Total_Failed_count": failed_count,
                "Excel_value_count": Excel_count,
                "HUB_Value_count": Hub_count,
            }

            return mapping
    except Exception as e:
        logger.warning("Error inside Filtering Function:", e)
        print("Error inside Filtering Function:", e)
        message = "Error in Filteration"
        return message


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

    # tenant database filtering function------------------------------------------------
    # def tenant_filtering(start_date, end_date, tenant_service_id, hub_service_id):
    # logger.info("Entered Tenant filtering function")
    # result = pd.DataFrame()

    # Prepare a safe, parameterized query
    # query = text(
    #     """
    #     WITH cte AS (
    #         SELECT src.Id as TENANT_ID,
    #                src.UserName as IHUB_USERNAME,
    #                src.TranAmountTotal as AMOUNT,
    #                src.TransactionStatus as TENANT_STATUS,
    #                src.CreationTs as SERVICE_DATE,
    #                src.VendorSubServiceMappingId,
    #                hub.Id AS hub_id
    #         FROM (
    #             SELECT mt.*, u.UserName
    #             FROM tenantinetcsc.MasterTransaction mt
    #             LEFT JOIN tenantinetcsc.EboDetail ed ON ed.id = mt.EboDetailId
    #             LEFT JOIN tenantinetcsc.`User` u ON u.Id = ed.UserId
    #             WHERE DATE(mt.CreationTs) BETWEEN :start_date AND :end_date
    #             AND mt.VendorSubServiceMappingId IN :tenant_service_id

    #             UNION ALL

    #             SELECT umt.*, u.UserName
    #             FROM tenantupcb.MasterTransaction umt
    #             LEFT JOIN tenantupcb.EboDetail ed ON ed.id = umt.EboDetailId
    #             LEFT JOIN tenantupcb.`User` u ON u.Id = ed.UserId
    #             WHERE DATE(umt.CreationTs) BETWEEN :start_date AND :end_date
    #             AND umt.VendorSubServiceMappingId IN :tenant_service_id

    #             UNION ALL

    #             SELECT imt.*, u.UserName
    #             FROM tenantiticsc.MasterTransaction imt
    #             LEFT JOIN tenantiticsc.EboDetail ed ON ed.id = imt.EboDetailId
    #             LEFT JOIN tenantiticsc.`User` u ON u.Id = ed.UserId
    #             WHERE DATE(imt.CreationTs) BETWEEN :start_date AND :end_date
    #             AND imt.VendorSubServiceMappingId IN :tenant_service_id
    #         ) AS src
    #         LEFT JOIN ihubcore.MasterTransaction AS hub
    #         ON hub.TenantMasterTransactionId = src.Id
    #         AND DATE(hub.CreationTs) BETWEEN :start_date AND :end_date
    #         AND hub.VendorSubServiceMappingId IN :hub_service_id
    #     )
    #     SELECT *
    #     FROM cte
    #     WHERE hub_id IS NULL
    # """
    # )
    # tenant_service_id = (
    #     [tenant_service_id] if isinstance(tenant_service_id, int) else tenant_service_id
    # )
    # hub_service_id = (
    #     [hub_service_id] if isinstance(hub_service_id, int) else hub_service_id
    # )

    # # Convert lists to tuples for SQLAlchemy to treat them correctly in IN clauses
    # params = {
    #     "start_date": start_date,
    #     "end_date": end_date,
    #     "tenant_service_id": tuple(tenant_service_id),
    #     "hub_service_id": tuple(hub_service_id),
    # }
    # print(params)
    # try:
    #     result = execute_sql_with_retry(query, params=params)
    # except SQLAlchemyError as e:
    #     logger.error(f"Database error in Tenant DB Filtering: {e}")
    # except Exception as e:
    #     logger.error(f"Unexpected error in Tenant DB Filtering: {e}")

    # return result


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

        df_db[f"{service_name}_STATUS"] = df_db["service_status"].apply(
            lambda x: status_mapping.get(x, x)
        )
        df_db.drop(columns=["service_status"], inplace=True)
        df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
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
        ebo_result = get_ebo_wallet_data(start_date, end_date)
        if ebo_result is not None and not ebo_result.empty:
            result = pd.merge(
                df_db,
                ebo_result,
                how="left",
                left_on="IHUB_REFERENCE",
                right_on="IHubReferenceId",
                validate="one_to_one",
            )
        else:
            logger.warning("No EBO Wallet data returned")
            result = df_db

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
            END AS IHUB_LEDGER_STATUS
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
        df_db[f"{service_name}_STATUS"] = df_db["service_status"].apply(
            lambda x: status_mapping.get(x, x)
        )
        df_db.drop(columns=["service_status"], inplace=True)
        df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
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
        ebo_result = get_ebo_wallet_data(start_date, end_date)
        if ebo_result is not None and not ebo_result.empty:
            result = pd.merge(
                df_db,
                ebo_result,
                how="left",
                left_on="IHUB_REFERENCE",
                right_on="IHubReferenceId",
                validate="one_to_one",
            )
        else:
            logger.warning("No EBO Wallet data returned")
            result = df_db

    except SQLAlchemyError as e:
        logger.error(f"Database error in Matm_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in Matm_Service(): {e}")

    return result


# ----------------------------------------------------------------------------------
#
