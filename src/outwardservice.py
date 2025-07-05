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


# -----------------------------------------------------------------------------
# service function selection
def outward_service_selection(start_date, end_date, service_name, df_excel):
    logger.info(f"Entering Reconciliation for {service_name} Service")
    start_date = start_date
    end_date = end_date
    result = None
    if service_name == "RECHARGE":
        if "REFID" in df_excel:
            logger.info("Recharge service: Column 'REFID' renamed to 'REFID'")
            # tenant_service_id = getServiceId(5, 6)
            # # print(tenant_service_id)
            # if not tenant_service_id.empty:
            #     tenant_service_ids = tenant_service_id["id"].tolist()
            #     Hub_service_id = ",".join(str(x) for x in tenant_service_ids)
            #     tenant_service_id = Hub_service_id
            #     # print("Hub_service_id:", Hub_service_id)
            # else:
            #     tenant_service_id = ""
            #     print("No service IDs found.")
            # Hub_service_id = ",".join(str(x) for x in Hub_service_id)
            hub_data = recharge_Service(start_date, end_date, service_name)
            # tenant_data = tenant_filtering(
            #     start_date, end_date, tenant_service_id, Hub_service_id
            # )
            result = filtering_Data(hub_data, df_excel, service_name)
        else:
            logger.warning("Wrong File Uploaded in Recharge Service")
            message = "Wrong File Updloaded...!"
            return message
    elif service_name == "BBPS":
        if "REFID" in df_excel:
            df_excel = df_excel.rename(
                columns={
                    "Transaction Ref ID": "REFID",
                    "NPCI Transaction Desc": "VENDOR_STATUS",
                }
            )
            values_mapping = {
                "Successful": "success",
                "Failure": "failed",
                "Transaction timed out": "failed",
                "": "failed",
            }
            df_excel["VENDOR_STATUS"] = (
                df_excel["VENDOR_STATUS"]
                .fillna("failed")
                .apply(lambda x: values_mapping.get(x, "failed"))
            )
            # tenant_service_id = getServiceId(1, 2)
            # print(tenant_service_id)
            # if not tenant_service_id.empty:
            #     tenant_service_ids = tenant_service_id["id"].tolist()
            #     Hub_service_id = ",".join(str(x) for x in tenant_service_ids)
            #     tenant_service_id = Hub_service_id
            #     # print("Hub_service_id:", Hub_service_id)
            # else:
            #     tenant_service_id = ""
            hub_data = Bbps_service(start_date, end_date, service_name)
            # tenant_data = tenant_filtering(
            #     start_date, end_date, tenant_service_id, Hub_service_id
            # )
            result = filtering_Data(hub_data, df_excel, service_name)
        else:
            logger.warning("Wrong File Uploaded BBPS Service")
            message = "Wrong File Updloaded...!"
            return message
    elif service_name == "PASSPORT":
        if "Value Date" in df_excel:
            # values_mapping = {
            #     "Appointment fixed": "success",
            #     "HOLD": "success",
            #     "REJECTED": "failed",
            # }
            # df_excel["VENDOR_STATUS"] = (
            #     df_excel["VENDOR_STATUS"]
            #     .fillna("unknown")
            #     .apply(lambda x: values_mapping.get(x, x))
            # )
            hub_data = passport_service(start_date, end_date, service_name)
            result = filtering_Data(hub_data, df_excel, service_name)
        else:
            logger.warning("Wrong File Uploaded Passport service")
            message = "Wrong File Updloaded...!"
            return message
    elif service_name == "LIC":
        if "IMWTID" in df_excel:
            df_cleaned = df_excel[
                ~(
                    df_excel["REFID"].isna()
                    & df_excel["IMWTID"].isna()
                    & df_excel["OPERATORID"].isna()
                )
            ].copy()
            hub_data = lic_service(start_date, end_date, service_name)

            result = filtering_Data(hub_data, df_cleaned, service_name)
        else:
            logger.warning("Wrong File Uploaded LIC Service")
            message = "Wrong File Updloaded...!"
            return message
    elif service_name == "PANUTI":
        if "Trans No" in df_excel:
            # tenant_service_id = 201
            # Hub_service_id = 218
            df_excel["VENDOR_STATUS"] = df_excel["VENDOR_STATUS"].astype(str)
            df_excel["VENDOR_STATUS"] = df_excel["VENDOR_STATUS"].apply(
                lambda x: "failed" if "refunded" in x.lower() else "success"
            )
            hub_data = Panuti_service(start_date, end_date, service_name)
            result = filtering_Data(hub_data, df_excel, service_name)
        else:
            logger.warning("Wrong File Uploaded PANUTI Service")
            message = "Wrong File Updloaded...!"
            return message
    elif service_name == "ASTRO":
        if "REFID" in df_excel:
            # tenant_service_id = 201
            # Hub_service_id = 218
            values_mapping = {
                "Processed": "success",
                "Rolled Back": "intiated",
            }
            df_excel["VENDOR_STATUS"] = (
                df_excel["VENDOR_STATUS"]
                .fillna("unknown")
                .apply(lambda x: values_mapping.get(x, x))
            )
            hub_data = astro_service(start_date, end_date, service_name)
            result = filtering_Data(hub_data, df_excel, service_name)
        else:
            logger.warning("Wrong File Uploaded in Asrto service")
            message = "Wrong File Updloaded...!"
            return message

    else:
        logger.warning("OutwardService  function selection Error ")
        message = "Service Name Error..!"
        return message

    return result


# ---------------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------------
# Filtering Function
def filtering_Data(df_db, df_excel, service_name):
    logger.info(f"Filteration Starts for {service_name} service")
    Excel_count = len(df_excel)
    Hub_count = df_db.shape[0]
    mapping = None
    # converting the date of both db and excel to string
    df_db["SERVICE_DATE"] = df_db["SERVICE_DATE"].dt.strftime("%Y-%m-%d")
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

    # Renaming Col in Excel
    df_excel = df_excel.rename(columns={"STATUS": "VENDOR_STATUS"})

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
        "BILL_FETCH_STATUS",
        # "TENANT_LEDGER_STATUS",
        "TRANSACTION_CREDIT",
        "TRANSACTION_DEBIT",
        "COMMISSION_CREDIT",
        "COMMISSION_REVERSAL",
    ]

    # 1 Filtering Data initiated in IHUB portal and not in Vendor Xl
    not_in_vendor = df_db[~df_db["VENDOR_REFERENCE"].isin(df_excel["REFID"])].copy()
    not_in_vendor["CATEGORY"] = "NOT_IN_VENDOR"
    not_in_vendor = not_in_vendor.rename(columns={"VENDOR_REFERENCE": "REFID"})
    not_in_vendor = safe_column_select(not_in_vendor, required_columns)
    # print(not_in_vendor["REFID"])

    # 2. Filtering Data Present in Vendor XL but Not in Ihub Portal
    not_in_portal = df_excel[~df_excel["REFID"].isin(df_db["VENDOR_REFERENCE"])].copy()
    not_in_portal["CATEGORY"] = "NOT_IN_PORTAL"
    not_in_portal = safe_column_select(not_in_portal, required_columns)

    # 4. Filtering Data that matches in both Ihub Portal and Vendor Xl as : Matched
    matched = df_db.merge(
        df_excel, left_on="VENDOR_REFERENCE", right_on="REFID", how="inner"
    ).copy()
    matched["CATEGORY"] = "MATCHED"
    matched = safe_column_select(matched, required_columns)
    print(matched[["IHUB_MASTER_STATUS", "VENDOR_STATUS"]].head(10))

    # 5. Filtering Data that Mismatched in both Ihub Portal and Vendor Xl as : Mismatched
    mismatched = matched[
        matched[f"IHUB_MASTER_STATUS"].str.lower()
        != matched["VENDOR_STATUS"].str.lower()
    ].copy()
    mismatched["CATEGORY"] = "MISMATCHED"
    mismatched = safe_column_select(mismatched, required_columns)
    print(matched.shape[0])
    # 6. Getting total count of success and failure data
    matched_success_status = matched[
        (matched["IHUB_MASTER_STATUS"].str.lower() == "success")
        & (matched["VENDOR_STATUS"].str.lower() == "success")
    ]
    print(matched_success_status[["IHUB_MASTER_STATUS", "VENDOR_STATUS"]])
    success_count = matched_success_status.shape[0]
    print(success_count)
    matched_failed_status = matched[
        (matched["IHUB_MASTER_STATUS"].str.lower() == "failed")
        & (matched["VENDOR_STATUS"].str.lower() == "failed")
    ]
    # print(matched_failed_status["REFID"])
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
    # SCENARIO 4 IHUB_FAIL_VEND_FAIL-NIL
    ihub_vend_fail_not_in_ledger = matched[
        (matched["VENDOR_STATUS"].str.lower() == "failed")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "failed")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
    ].copy()
    ihub_vend_fail_not_in_ledger["CATEGORY"] = "IHUB_FAIL_VEND_FAIL-NIL"
    ihub_vend_fail_not_in_ledger = safe_column_select(
        ihub_vend_fail_not_in_ledger, required_columns
    )
    # SCENARIO 5 IHUB_INT_VEND_SUC-NIL
    ihub_initiate_vend_succes_not_in_ledger = matched[
        (matched["VENDOR_STATUS"].str.lower() == "success")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "initiated")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
    ].copy()
    ihub_initiate_vend_succes_not_in_ledger["CATEGORY"] = "IHUB_INT_VEND_SUC-NIL"
    ihub_initiate_vend_succes_not_in_ledger = safe_column_select(
        ihub_initiate_vend_succes_not_in_ledger, required_columns
    )
    # SCENARIO 6 VEND_FAIL_IHUB_INT-NIL
    ihub_initiate_vend_fail_not_in_ledger = matched[
        (matched["VENDOR_STATUS"].str.lower() == "failed")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "initiated")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
    ].copy()
    ihub_initiate_vend_fail_not_in_ledger["CATEGORY"] = " VEND_FAIL_IHUB_INT-NIL"
    ihub_initiate_vend_fail_not_in_ledger = safe_column_select(
        ihub_initiate_vend_fail_not_in_ledger, required_columns
    )

    # SCENARIO 1 VEND_IHUB_SUC IL
    #    vend_ihub_succ = matched[
    #        (matched["VENDOR_STATUS"].str.lower() == "success")
    #        & (matched["IHUB_MASTER_STATUS"].str.lower() == "success")
    #        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
    #    ].copy()
    #    vend_ihub_succ["CATEGORY"] = "VEND_IHUB_SUC"
    #    vend_ihub_succ = safe_column_select(
    #         vend_ihub_succ, required_columns
    #    )
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
        & (matched["IHUB_MASTER_STATUS"].str.lower().isin(["initiated", "inprogress"]))
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
    ].copy()
    ihub_initiate_vend_succes["CATEGORY"] = "IHUB_INT_VEND_SUC"
    ihub_initiate_vend_succes = safe_column_select(
        ihub_initiate_vend_succes, required_columns
    )

    # SCENARIO 6 VEND_FAIL_IHUB_INT IL
    ihub_initiate_vend_fail = matched[
        (matched["VENDOR_STATUS"].str.lower() == "failed")
        & (matched["IHUB_MASTER_STATUS"].str.lower().isin(["initiated", "inprogress"]))
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
    ].copy()
    ihub_initiate_vend_fail["CATEGORY"] = "VEND_FAIL_IHUB_INT"
    ihub_initiate_vend_fail = safe_column_select(
        ihub_initiate_vend_fail, required_columns
    )
    # Scenario Block ends-----------------------------------------------------------------------------------

    # tenant_data["CATEGORY"] = "TENANT_DB_INTI - NOT_IN_IHUB"
    # print(not_in_vendor)
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
        ihub_vend_fail_not_in_ledger,
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
            "IHUB_VEND_FAIL-NIL": ihub_vend_fail_not_in_ledger,
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
            "Excel_value_count":Excel_count,
            "HUB_Value_count":Hub_count,
        }
        # print(mapping)
        return mapping


# Filteration Function Ends-------------------------------------------------------------------


# Ebo Wallet Amount and commission  Debit credit check function  -------------------------------------------
def get_ebo_wallet_data(start_date, end_date):
    logger.info("Fetching Data from EBO Wallet Transaction")
    ebo_df = None
    query = text(
        """
        SELECT  
            mt2.TransactionRefNum,
            ewt.MasterTransactionsId,
            MAX(CASE WHEN ewt.Description IN ('Transaction - Credit','Transaction - Credit due to failure') THEN 'Yes' ELSE 'No' END) AS TRANSACTION_CREDIT,
            MAX(CASE WHEN ewt.Description = 'Transaction - Debit' THEN 'Yes' ELSE 'No' END) AS TRANSACTION_DEBIT,
            MAX(CASE WHEN ewt.Description = 'Commission Added' THEN 'Yes' ELSE 'No' END) AS COMMISSION_CREDIT,
            MAX(CASE WHEN ewt.Description = 'Commission - Reversal' THEN 'Yes' ELSE 'No' END) AS COMMISSION_REVERSAL
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


# ----------------------------------------------------------------------------------


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


# -----------------------------------------------------------------------------


# Recharge service function ---------------------------------------------------
def recharge_Service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()  # Initialize as empty DataFrame

    # Use parameterized query to prevent SQL injection
    query = text(
        f"""
        SELECT mt2.TransactionRefNum AS IHUB_REFERENCE,
               mt2.TenantDetailId as TENANT_ID,   
               sn.requestID AS VENDOR_REFERENCE,
               u.UserName as IHUB_USERNAME, 
               mt2.TransactionStatus AS IHUB_MASTER_STATUS,
               sn.CreationTs AS SERVICE_DATE, 
               sn.rechargeStatus AS service_status,
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
        LEFT JOIN tenantinetcsc.EboDetail ed ON mt2.EboDetailId = ed.Id
        LEFT JOIN tenantinetcsc.`User` u ON u.Id = ed.UserId
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND CURRENT_DATE()
        ) iwt ON iwt.IHubReferenceId = mt2.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND CURRENT_DATE()
        ) twt ON twt.IHubReferenceId = mt2.TransactionRefNum
        WHERE DATE(sn.CreationTs) BETWEEN :start_date AND :end_date
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
            2: "pending",
            3: "failed",
            4: "instant failed",
        }
        df_db[f"{service_name}_STATUS"] = df_db["service_status"].apply(
            lambda x: status_mapping.get(x, x)
        )
        df_db.drop(columns=["service_status"], inplace=True)
        # Tenant ID mapping
        tenant_Id_mapping = {
            1: "INET-CSC",
            2: "ITI-ESEVA",
            3: "UPCB",
        }
        df_db["TENANT_ID"] = (
            df_db["TENANT_ID"].map(tenant_Id_mapping).fillna(df_db["TENANT_ID"])
        )
        # Get wallet data with retry
        ebo_result = get_ebo_wallet_data(start_date, end_date)

        if ebo_result is not None and not ebo_result.empty:
            result = pd.merge(
                df_db,
                ebo_result,
                how="left",
                left_on="IHUB_REFERENCE",
                right_on="TransactionRefNum",
                validate="one_to_one",  # Add validation
            )
        else:
            logger.warning("No ebo wallet data returned")
            result = df_db

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
    query = text(
        f"""
         SELECT
        mt2.TransactionRefNum as IHUB_REFERENCE,
        u.Username as IHUB_USERNAME,
        bbp.TxnRefId  as VENDOR_REFERENCE,
        bbp.Amount as AMOUNT,
        bbp.creationTs as SERVICE_DATE,
        mt2.TransactionStatus AS IHUB_MASTER_STATUS,
        mt2.tenantDetailID as TENANT_ID,
        bbp.TransactionStatusType as service_status ,bbp.HeadReferenceId ,
        CASE when iw.IHubReferenceId IS NOT NULL THEN 'Yes'
        ELSE 'NO'
        END AS IHUB_LEDGER_STATUS,
        CASE when bf.id IS NOT NULL THEN 'Yes'
        ELSE 'NO'
        END AS BILL_FETCH_STATUS
        FROM  ihubcore.MasterTransaction mt2
        LEFT JOIN 
        ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN 
        ihubcore.BBPS_BillPay bbp ON bbp.MasterSubTransactionId = mst.Id 
        left join tenantinetcsc.EboDetail ed on ed.Id = mt2.EboDetailId 
        left join tenantinetcsc.`User` u  on u.id = ed.UserId 
        left join (Select DISTINCT iwt.IHubReferenceId from ihubcore.IHubWalletTransaction iwt 
        where date(iwt.creationTs) between  :start_date AND :end_date ) as iw 
        on iw.IHubReferenceId =mt2.TransactionRefNum 
        left join (select DISTINCT bbf.id  from ihubcore.BBPS_BillFetch bbf 
        where date(bbf.creationTs) between  :start_date AND :end_date) as bf 
        on bf.id  = bbp.BBPS_BillFetchId 
        WHERE DATE(bbp.CreationTs) BETWEEN  :start_date AND :end_date 
        
        """
    )
    params = {"start_date": start_date, "end_date": end_date}
    try:
        df_db = execute_sql_with_retry(
            query,
            params=params,
        )
        if df_db.empty:
            logger.warning(f"No data returned for service:{service_name}")
            return pd.DataFrame()

        # mapping status name with enum
        status_mapping = {
            0: "unknown",
            1: "success",
            2: "failed",
            3: "inprogress",
            4: "partialsuccuess",
        }
        df_db[f"{service_name}_STATUS"] = df_db["service_status"].apply(
            lambda x: status_mapping.get(x, x)
        )
        df_db.drop(columns=["service_status"], inplace=True)
        # calling filtering function
        tenant_Id_mapping = {
            1: "INET-CSC",
            2: "ITI_ESEVA",
            3: "UPCB",
        }
        df_db["TENANT_ID"] = (
            df_db["TENANT_ID"].map(tenant_Id_mapping).fillna(df_db["TENANT_ID"])
        )
        ebo_result = get_ebo_wallet_data(start_date, end_date)
        if ebo_result is not None and not ebo_result.empty:
            result = pd.merge(
                df_db,
                ebo_result,
                how="left",
                left_on="IHUB_REFERENCE",
                right_on="TransactionRefNum",
                validate="one_to_one",
            )
        else:
            logger.warning("No ebo wallet data returned")
            result = df_db

    except SQLAlchemyError as e:
        logger.error(f"Databasr error in PAN_UTI_SERVICE():{e}")
    except Exception as e:
        logger.error(f"Unexpected error in PAN_UTI_SERVICE():{e} ")
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
               u.UserName as IHUB_USERNAME, 
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
        LEFT JOIN tenantinetcsc.EboDetail ed ON mt2.EboDetailId = ed.Id
        LEFT JOIN tenantinetcsc.`User` u ON u.Id = ed.UserId
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

    # Reading data from Server
    try:
        df_db = execute_sql_with_retry(
            query,
            params=params,
        )
        if df_db.empty:
            logger.warning(f"No data returned for service:{service_name}")
            return pd.DataFrame()
        # mapping status name with enum
        status_mapping = {
            0: "failed",
            1: "success",
        }
        df_db[f"{service_name}_STATUS"] = df_db["service_status"].apply(
            lambda x: status_mapping.get(x, x)
        )
        df_db.drop(columns=["service_status"], inplace=True)
        # calling filtering function
        tenant_Id_mapping = {
            1: "INET-CSC",
            2: "ITI_ESEVA",
            3: "UPCB",
        }
        df_db["TENANT_ID"] = (
            df_db["TENANT_ID"].map(tenant_Id_mapping).fillna(df_db["TENANT_ID"])
        )
        ebo_result = get_ebo_wallet_data(start_date, end_date)
        if ebo_result is not None and not ebo_result.empty:
            result = pd.merge(
                df_db,
                ebo_result,
                how="left",
                left_on="IHUB_REFERENCE",
                right_on="TransactionRefNum",
                validate="one_to_one",
            )
        else:
            logger.warning("No ebo wallet data returned")
            result = df_db

    except SQLAlchemyError as e:
        logger.error(f"Databasr error in PAN_UTI_SERVICE():{e}")
    except Exception as e:
        logger.error(f"Unexpected error in PAN_UTI_SERVICE():{e} ")
    return result


# ----------------------------------------------------------------------------------------
# IMT SERVICE FUNCTION-------------------------------------------------------------------
def IMT_Service(start_date, end_date, df_excel, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = f"""
            SELECT mt2.TransactionRefNum AS IHUB_REFERENCE,
            pst.VendorReferenceId as VENDOR_REFERENCE,
            u.UserName as IHUB_USERNAME,
            mt2.TransactionStatus AS IHUB_MASTER_STATUS,
            pst.PaySprintTransStatus as {service_name}_STATUS,
            CASE
            WHEN a.IHubReferenceId  IS NOT NULL THEN 'Yes'
            ELSE 'No'
            END AS IHUB_LEDGER_STATUS
            FROM
            ihubcore.MasterTransaction mt2
            LEFT JOIN
            ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt2.Id
            LEFT JOIN
            ihubcore.PaySprint_Transaction pst ON pst.MasterSubTransactionId = mst.Id
            LEFT JOIN
            tenantinetcsc.EboDetail ed ON mt2.EboDetailId = ed.Id
            LEFT JOIN
            tenantinetcsc.`User` u ON u.id = ed.UserId
            LEFT JOIN
            (SELECT DISTINCT iwt.IHubReferenceId AS IHubReferenceId
            FROM ihubcore.IHubWalletTransaction iwt
            WHERE DATE(iwt.CreationTs) BETWEEN '{start_date}' AND CURRENT_DATE()
            ) a ON a.IHubReferenceId = mt2.TransactionRefNum
            WHERE
            DATE(pst.CreationTs) BETWEEN '{start_date}' AND '{end_date}' 
            """
    # Reading data from Server
    df_db = pd.read_sql(query, con=engine)
    df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
    df_excel["REFID"] = df_excel["REFID"].astype(str)
    refunded_trans_ids = df_excel[df_excel["STATUS"].isin(["Refunded", "Failed"])]
    # print(refunded_trans_ids)
    # Extract the REFID column as a list of strings (safely handling quotes)
    refunded_ids_list = refunded_trans_ids["REFID"].astype(str).tolist()
    # Joining the list into a properly quoted string for SQL
    # refunded_ids_string = "\n   UNION ALL\n   ".join(
    # f"SELECT '{refid}' AS TransactionRefNumVendor" for refid in refunded_ids_list)
    refunded_ids_string = ",".join(f"'{refid}'" for refid in refunded_ids_list)
    # print(refunded_ids_string)

    query = f"""
        SELECT pst.VendorReferenceId,
        CASE WHEN mr.MasterSubTransactionId IS NOT NULL THEN 'refunded'
        ELSE 'not_refunded'
        END AS IHUB_REFUND_STATUS
        FROM ihubcore.PaySprint_Transaction pst
        LEFT JOIN ihubcore.MasterRefund mr
        ON mr.MasterSubTransactionId = pst.MasterSubTransactionId
        WHERE pst.VendorReferenceId IN ({refunded_ids_string})
        AND DATE(pst.creationTs) BETWEEN "{start_date}" AND "{end_date}"
        """
    # print(query)
    refunded_db = pd.read_sql(query, con=engine)
    # print(refunded_db)
    refunded_db["VendorReferenceId"] = refunded_db["VendorReferenceId"].astype(str)
    merged_df = df_db.merge(
        refunded_db,
        how="left",
        left_on="VENDOR_REFERENCE",
        right_on="VendorReferenceId",
    )
    merged_df.drop(columns=["VendorReferenceId"], inplace=True)
    df_db = merged_df
    df_db["IHUB_REFUND_STATUS"] = df_db["IHUB_REFUND_STATUS"].fillna("not_applicable")
    # print(df_db)
    # df_db.to_excel("C:\\Users\\Sathyaswaruban\\Documents\\IMT.xlsx", index=False)
    # df_excel['STATUS'] = df_excel['STATUS'].replace('Refunded', 'failed')
    logger.info(
        "Refunded status in IMT excel renamed to failed since IHUB portal don't have refunded status"
    )
    # mapping status name with enums
    status_mapping = {
        0: "unknown",
        1: "success",
        2: "failed",
        3: "inprogress",
        4: "partialsuccuess",
    }
    df_db[f"{service_name}_STATUS"] = df_db[f"{service_name}_STATUS"].apply(
        lambda x: status_mapping.get(x, x)
    )
    # calling filtering function
    result = filtering_Data(df_db, df_excel, service_name)
    return result


# -------------------------------------------------------------------


# ------------------------------------------------------------------------
# PAN-NSDL Service function
def Pannsdl_service(start_date, end_date, df_excel, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = f"""
        select pit.AcknowledgeNo as VENDOR_REFERENCE,mt.TransactionRefNum AS IHUB_REFERENCE,
        mt.TransactionStatus AS IHUB_MASTER_STATUS,
        u.Username as IHUB_USERNAME,
        pit.applicationstatus as {service_name}_STATUS,
        CASE When 
        a.IHubReferenceId IS NOT NULL THEN 'Yes'
        ELSE 'NO'
        END AS 'IHUB_LEDGER_STATUS'
        from ihubcore.PanInTransaction pit
        left join ihubcore.MasterSubTransaction mst on mst.id= pit.MasterSubTransactionId
        left join ihubcore.MasterTransaction mt on mt.id = mst.MasterTransactionId
        left join tenantinetcsc.EboDetail ed on ed.Id = mt.EboDetailId 
        left join tenantinetcsc.`User` u  on u.id = ed.UserId 
        left join (select DISTINCT iwt.IHubReferenceId  from ihubcore.IHubWalletTransaction iwt  
        WHERE Date(iwt.creationTs) BETWEEN '{start_date}'AND CURRENT_DATE()) a 
        on a.IHubReferenceId = mt.TransactionRefNum
        where DATE(u.CreationTs) BETWEEN '{start_date}' and '{end_date}
        """
    # Reading data from Server
    df_db = pd.read_sql(query, con=engine)
    # mapping status name with enum
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
    df_db[f"{service_name}_STATUS"] = df_db[f"{service_name}_STATUS"].apply(
        lambda x: status_mapping.get(x, x)
    )
    # calling filtering function
    result = filtering_Data(df_db, df_excel, service_name)
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
               u.UserName as IHUB_USERNAME, 
               mt2.TransactionStatus AS IHUB_MASTER_STATUS,
               pi.BankReferenceTs AS SERVICE_DATE, 
               pi.PassportInStatusType AS service_status,
               pi.Amount as AMOUNT,
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
        LEFT JOIN tenantinetcsc.EboDetail ed ON mt2.EboDetailId = ed.Id
        LEFT JOIN tenantinetcsc.`User` u ON u.Id = ed.UserId
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
            0: "unknown",
            1: "NewRequest",
            2: "CallMade",
            3: "AppointmentFixed",
            4: "ApplicationClosed",
            5: "ApplicationRejected",
            6: "ReAppointment",
        }
        df_db[f"{service_name}_STATUS"] = df_db["service_status"].apply(
            lambda x: status_mapping.get(x, x)
        )
        df_db.drop(columns=["service_status"], inplace=True)
        # Tenant ID mapping
        tenant_Id_mapping = {
            1: "INET-CSC",
            2: "ITI-ESEVA",
            3: "UPCB",
        }
        df_db["TENANT_ID"] = (
            df_db["TENANT_ID"].map(tenant_Id_mapping).fillna(df_db["TENANT_ID"])
        )
        # Get wallet data with retry
        ebo_result = get_ebo_wallet_data(start_date, end_date)

        if ebo_result is not None and not ebo_result.empty:
            result = pd.merge(
                df_db,
                ebo_result,
                how="left",
                left_on="IHUB_REFERENCE",
                right_on="TransactionRefNum",
                validate="one_to_one",  # Add validation
            )
        else:
            logger.warning("No ebo wallet data returned")
            result = df_db

    except SQLAlchemyError as e:
        logger.error(f"Database error in recharge_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in recharge_Service(): {e}")

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
               u.UserName as IHUB_USERNAME, 
               mt2.TransactionStatus AS IHUB_MASTER_STATUS,
               lpt.CreationTs AS SERVICE_DATE, 
               lpt.BillPayStatus AS service_status,
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
        LEFT JOIN ihubcore.LicPremiumTransaction lpt ON lpt.MasterSubTransactionId = mst.Id
        LEFT JOIN tenantinetcsc.EboDetail ed ON mt2.EboDetailId = ed.Id
        LEFT JOIN tenantinetcsc.`User` u ON u.Id = ed.UserId
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
        df_db[f"{service_name}_STATUS"] = df_db["service_status"].apply(
            lambda x: status_mapping.get(x, x)
        )
        df_db.drop(columns=["service_status"], inplace=True)
        # Tenant ID mapping
        tenant_Id_mapping = {
            1: "INET-CSC",
            2: "ITI-ESEVA",
            3: "UPCB",
        }
        df_db["TENANT_ID"] = (
            df_db["TENANT_ID"].map(tenant_Id_mapping).fillna(df_db["TENANT_ID"])
        )
        # Get wallet data with retry
        ebo_result = get_ebo_wallet_data(start_date, end_date)

        if ebo_result is not None and not ebo_result.empty:
            result = pd.merge(
                df_db,
                ebo_result,
                how="left",
                left_on="IHUB_REFERENCE",
                right_on="TransactionRefNum",
                validate="one_to_one",  # Add validation
            )
        else:
            logger.warning("No ebo wallet data returned")
            result = df_db

    except SQLAlchemyError as e:
        logger.error(f"Database error in recharge_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in recharge_Service(): {e}")

    return result


# ----------------------------------------------------------------------
# Stro service Function
def astro_service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    query = text(
        f"""
        SELECT mt2.TransactionRefNum AS IHUB_REFERENCE,
               mt2.TenantDetailId as TENANT_ID,   
               at2.OrderId AS VENDOR_REFERENCE,
               u.UserName as IHUB_USERNAME, 
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
        LEFT JOIN tenantinetcsc.EboDetail ed ON mt2.EboDetailId = ed.Id
        LEFT JOIN tenantinetcsc.`User` u ON u.Id = ed.UserId
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
            1: "unknown",
            2: "success",
        }
        df_db[f"{service_name}_STATUS"] = df_db["service_status"].apply(
            lambda x: status_mapping.get(x, x)
        )
        df_db.drop(columns=["service_status"], inplace=True)
        # Tenant ID mapping
        tenant_Id_mapping = {
            1: "INET-CSC",
            2: "ITI-ESEVA",
            3: "UPCB",
        }
        df_db["TENANT_ID"] = (
            df_db["TENANT_ID"].map(tenant_Id_mapping).fillna(df_db["TENANT_ID"])
        )
        # Get wallet data with retry
        ebo_result = get_ebo_wallet_data(start_date, end_date)

        if ebo_result is not None and not ebo_result.empty:
            result = pd.merge(
                df_db,
                ebo_result,
                how="left",
                left_on="IHUB_REFERENCE",
                right_on="TransactionRefNum",
                validate="one_to_one",  # Add validation
            )
        else:
            logger.warning("No ebo wallet data returned")
            result = df_db

    except SQLAlchemyError as e:
        logger.error(f"Database error in recharge_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in recharge_Service(): {e}")

    return result
