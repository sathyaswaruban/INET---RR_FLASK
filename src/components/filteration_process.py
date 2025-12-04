import pandas as pd
from logger_config import logger
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from typing import Dict, Any, Optional
from logger_config import logger
from components.outwardservices import (
    recharge_Service,
    Bbps_service,
    Panuti_service,
    Pannsdl_service,
    dmt_Service,
    passport_service,
    lic_service,
    astro_service,
    insurance_offline_service,
    abhibus_service,
    moveToBank_service,
)
from components.inwardservice import matm_Service, aeps_Service

# Service configuration constants
SERVICE_CONFIGS = {
    "RECHARGE": {"required_columns": ["REFID"], "service_func": recharge_Service},
    "BBPS": {
        "status_mapping": {
            "Successful": "success",
            "Failure": "failed",
            "Transaction timed out": "timed out",
            "": "failed",
        },
        "service_func": Bbps_service,
    },
    "PASSPORT": {
        "service_func": passport_service,
    },
    "LIC": {
        "processing": lambda df: (
            df[
                ~(df["REFID"].isna() & df["IMWTID"].isna() & df["OPERATORID"].isna())
            ].copy()
            if all(col in df.columns for col in ["REFID", "IMWTID", "OPERATORID"])
            else df.copy()
        ),
        "service_func": lic_service,
    },
    "PANUTI": {
        "status_processing": lambda df: (
            df["VENDOR_STATUS"]
            .astype(str)
            .apply(lambda x: "failed" if "refunded" in x.lower() else "success")
            if "VENDOR_STATUS" in df.columns
            else None
        ),
        "service_func": Panuti_service,
    },
    "PANNSDL": {
        "status_processing": lambda df: (
            df["VENDOR_STATUS"]
            .astype(str)
            .apply(lambda x: "success" if "accepted" in x.lower() else "failed")
            if "VENDOR_STATUS" in df.columns
            else None
        ),
        "service_func": Pannsdl_service,
    },
    "ASTRO": {
        "status_mapping": {
            "Processed": "success",
            "Rolled Back": "intiated",
            "Not Processed": "failed",
        },
        "service_func": astro_service,
    },
    "DMT": {
        "status_mapping": {
            "Success": "success",
            "Refunded": "failed",
            "Failed": "failed",
        },
        "service_func": dmt_Service,
    },
    "INSURANCE_OFFLINE": {
        "service_func": insurance_offline_service,
    },
    "ABHIBUS": {
        "service_func": abhibus_service,
    },
    "AEPS": {
        "service_func": aeps_Service,
    },
    "MATM": {
        "processing": lambda df: (
            df[~(df["TID"].isna() & df["REFID"].isna() & df["DEVICE"].isna())].copy()
            if all(col in df.columns for col in ["TID", "REFID", "DEVICE"])
            else df.copy()
        ),
        "status_processing": lambda df: (
            df["VENDOR_STATUS"]
            .astype(str)
            .fillna("failed")
            .apply(lambda x: "success" if "auth_success" in x.lower() else "failed")
            if "VENDOR_STATUS" in df.columns
            else None
        ),
        "service_func": matm_Service,
    },
    "MOVETOBANK": {
        "status_mapping": {
            "PROCESSED": "success",
            "REJECTED": "failed",
        },
        "service_func": moveToBank_service,
    },
}


def process_status_column(
    df: pd.DataFrame, service_config: Dict[str, Any]
) -> pd.DataFrame:
    """Process status column based on service configuration"""
    if "status_mapping" in service_config:
        if "VENDOR_STATUS" in df.columns:
            df["VENDOR_STATUS"] = (
                df["VENDOR_STATUS"]
                .fillna("failed")
                .apply(lambda x: service_config["status_mapping"].get(x, "failed"))
            )
    elif "status_processing" in service_config:
        processed_status = service_config["status_processing"](df)
        if processed_status is not None:
            df["VENDOR_STATUS"] = processed_status
    return df


def service_selection(
    start_date: str,
    end_date: str,
    service_name: str,
    df_excel: pd.DataFrame,
    transaction_type: str,
) -> Any:
    """Handle outward and inward  service selection and processing."""
    logger.info(f"Entering Reconciliation for {service_name} Service")

    # Validate service name
    if service_name not in SERVICE_CONFIGS:
        logger.warning("OutwardService function selection Error")
        return "Service Name Error..!"

    service_config = SERVICE_CONFIGS[service_name]

    try:
        # Apply preprocessing if defined
        if "processing" in service_config:
            df_excel = service_config["processing"](df_excel)

        # Process status column
        df_excel = process_status_column(df_excel, service_config)
        pan_nsdl_iti_df = pd.DataFrame()

        # Get service data and filter
        if service_name == "AEPS":
            hub_data = service_config["service_func"](
                start_date, end_date, service_name, transaction_type
            )
        elif service_name == "PANNSDL":
            hub_data, pan_nsdl_iti_df = service_config["service_func"](
                start_date, end_date, service_name
            )
        else:
            hub_data = service_config["service_func"](
                start_date, end_date, service_name
            )
        return filtering_Data(hub_data, df_excel, service_name, pan_nsdl_iti_df)

    except Exception as e:
        logger.error(f"Error processing {service_name} service: {str(e)}")
        return f"Error processing {service_name} service"


# Unified filtering function for both inward and outward modules
def unified_filtering_data(
    df_db,
    pan_nsdl_iti_df,
    df_excel,
    service_name,
    status_column_db="IHUB_MASTER_STATUS",
    status_column_excel="VENDOR_STATUS",
    required_columns=None,
    extra_scenarios=None,
    ledger_status_col="IHUB_LEDGER_STATUS",
    bill_fetch_col=None,
    tenant_ledger_col=None,
    commission_cols=None,
    status_mapping_db=None,
    status_mapping_excel=None,
    mismatch_on=None,
    logger_obj=None,
):

    logger = logger_obj or globals().get("logger", None)
    log = logger.info if logger else print
    try:
        log(f"Filteration Starts for {service_name} service (unified)")
        Excel_count = len(df_excel)
        Hub_count = df_db.shape[0]
        # for col in ["VENDOR_REFERENCE", "IHUB_MASTER_STATUS"]:
        #     df_db[col] = df_db[col].astype(str).str.strip()
        # for col in ["REFID", "VENDOR_STATUS"]:
        #     df_excel[col] = df_excel[col].astype(str).str.strip()
        df_excel[status_column_excel] = (
            df_excel[status_column_excel].astype(str).str.strip()
        )
        df_excel["VENDOR_AMOUNT"] = df_excel["VENDOR_AMOUNT"].astype(float)
        # Preprocess dates
        if "VENDOR_DATE" in df_excel.columns:
            df_excel["VENDOR_DATE"] = pd.to_datetime(
                df_excel["VENDOR_DATE"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        if "SERVICE_DATE" in df_db.columns:
            df_db["SERVICE_DATE"] = pd.to_datetime(
                df_db["SERVICE_DATE"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        if (
            pan_nsdl_iti_df is not None
            and not pan_nsdl_iti_df.empty
            and "SERVICE_DATE" in pan_nsdl_iti_df.columns
        ):
            pan_nsdl_iti_df["SERVICE_DATE"] = pd.to_datetime(
                pan_nsdl_iti_df["SERVICE_DATE"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        # Map DB status if mapping provided
        if status_mapping_db and status_column_db in df_db.columns:
            df_db[status_column_db] = (
                df_db[status_column_db]
                .map(status_mapping_db)
                .fillna(df_db[status_column_db])
            )

        # Map Excel status if mapping provided
        if status_mapping_excel and status_column_excel in df_excel.columns:
            df_excel[status_column_excel] = (
                df_excel[status_column_excel]
                .map(status_mapping_excel)
                .fillna(df_excel[status_column_excel])
            )

        Hub_initiated_count = df_db[
            df_db[status_column_db]
            .astype(str)
            .str.lower()
            .isin(["initiated", "inprogress"])
        ].shape[0]
        Hub_success_count = df_db[
            df_db[status_column_db].astype(str).str.lower() == "success"
        ].shape[0]
        Hub_failed_count = df_db[
            df_db[status_column_db].astype(str).str.lower() == "failed"
        ].shape[0]
        Vendor_success_count = df_excel[
            df_excel[status_column_excel].astype(str).str.lower() == "success"
        ].shape[0]
        Vendor_failed_count = df_excel[
            df_excel[status_column_excel].astype(str).str.lower() == "failed"
        ].shape[0]
        Vendor_timeout_count = df_excel[
            df_excel[status_column_excel].astype(str).str.lower() == "timed out"
        ].shape[0]

        # Helper for column selection
        def safe_column_select(df, columns):
            existing_cols = [col for col in columns if col in df.columns]
            return df[existing_cols].copy()

        # Handle amount column renaming for not_in_vendor
        refid_list = df_excel["REFID"].dropna().astype(str).str.strip()
        not_in_vendor = df_db[
            (~df_db["VENDOR_REFERENCE"].astype(str).str.strip().isin(refid_list))
            | (df_db["VENDOR_REFERENCE"].isna())
            | (df_db["VENDOR_REFERENCE"].astype(str).str.strip() == "")
        ].copy()

        not_in_vendor["CATEGORY"] = "NOT_IN_VENDOR"
        not_in_vendor = not_in_vendor.rename(columns={"VENDOR_REFERENCE": "REFID"})

        not_in_vendor = safe_column_select(not_in_vendor, required_columns)
        # Handle amount column renaming for not_in_portal
        not_in_portal = df_excel[
            ~df_excel["REFID"].isin(df_db["VENDOR_REFERENCE"])
        ].copy()
        not_in_portal["CATEGORY"] = "NOT_IN_PORTAL"
        not_in_portal = safe_column_select(not_in_portal, required_columns)
        # ITI Matching for PANNSDL
        iti_Matched = pd.DataFrame()
        if service_name == "PANNSDL" and not pan_nsdl_iti_df.empty:
            pan_nsdl_iti_df["VENDOR_REFERENCE"] = (
                pan_nsdl_iti_df["VENDOR_REFERENCE"].astype(str).str.strip()
            )
            # ITI Matched
            iti_Matched = pan_nsdl_iti_df[
                pan_nsdl_iti_df["VENDOR_REFERENCE"].isin(not_in_portal["REFID"])
            ].copy()
            iti_Matched["CATEGORY"] = "MATCHED_IN_ITI"
            iti_Matched["service_status"] = (
                iti_Matched["service_status"]
                .astype(str)
                .str.lower()
                .replace({"1": "success", "2": "intiated", "3": "failed"})
            )
            iti_Matched = iti_Matched.rename(
                columns={
                    "VENDOR_REFERENCE": "REFID",
                    "service_status": "ITI_PAN_NSDL_STATUS",
                }
            )
            iti_Matched = iti_Matched.merge(
                df_excel, on="REFID", how="inner", suffixes=("_ITI", "_EXCEL")
            )
            iti_Matched = safe_column_select(iti_Matched, required_columns)
            not_in_portal = not_in_portal[
                ~not_in_portal["REFID"].isin(iti_Matched["REFID"])
            ]

        # Matched
        matched = df_db.merge(
            df_excel, left_on="VENDOR_REFERENCE", right_on="REFID", how="inner"
        ).copy()
        matched["CATEGORY"] = "MATCHED"
        matched = safe_column_select(matched, required_columns)
        # Mismatched
        mismatched = matched[
            matched[status_column_db].astype(str).str.lower()
            != matched[status_column_excel].astype(str).str.lower()
        ].copy()
        mismatched["CATEGORY"] = "MISMATCHED"
        mismatched = safe_column_select(mismatched, required_columns)

        # Scenario blocks (NIL and IL)
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
            "vend_ihub_succ_not_in_ledger": scenario_df(
                matched,
                (matched[status_column_excel].astype(str).str.lower() == "success")
                & (matched[status_column_db].astype(str).str.lower() == "success")
                & (matched[ledger_status_col].astype(str).str.lower() == "no"),
                "VEND_IHUB_SUC-NIL",
            ),
            "vend_fail_ihub_succ_not_in_ledger": scenario_df(
                matched,
                (matched[status_column_excel].astype(str).str.lower() == "failed")
                & (matched[status_column_db].astype(str).str.lower() == "success")
                & (matched[ledger_status_col].astype(str).str.lower() == "no"),
                "VEND_FAIL_IHUB_SUC-NIL",
            ),
            "vend_succ_ihub_fail_not_in_ledger": scenario_df(
                matched,
                (matched[status_column_excel].astype(str).str.lower() == "success")
                & (matched[status_column_db].astype(str).str.lower() == "failed")
                & (matched[ledger_status_col].astype(str).str.lower() == "no"),
                "VEND_SUC_IHUB_FAIL-NIL",
            ),
            "ihub_vend_fail_not_in_ledger": scenario_df(
                matched,
                (
                    matched[status_column_excel]
                    .astype(str)
                    .str.lower()
                    .isin(["failed", "timed out"])
                )
                & (matched[status_column_db].astype(str).str.lower() == "failed")
                & (matched[ledger_status_col].astype(str).str.lower() == "no"),
                "IHUB_FAIL_VEND_FAIL-NIL",
            ),
            "ihub_initiate_vend_succes_not_in_ledger": scenario_df(
                matched,
                (matched[status_column_excel].astype(str).str.lower() == "success")
                & (
                    matched[status_column_db]
                    .astype(str)
                    .str.lower()
                    .isin(["initiated", "inprogress", "pending"])
                )
                & (matched[ledger_status_col].astype(str).str.lower() == "no"),
                "IHUB_INT_VEND_SUC-NIL",
            ),
            "ihub_initiate_vend_fail_not_in_ledger": scenario_df(
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
                )
                & (matched[ledger_status_col].astype(str).str.lower() == "no"),
                "VEND_FAIL_IHUB_INT-NIL",
            ),
            "vend_ihub_succ": scenario_df(
                matched,
                (matched[status_column_excel].astype(str).str.lower() == "success")
                & (matched[status_column_db].astype(str).str.lower() == "success")
                & (matched[ledger_status_col].astype(str).str.lower() == "yes"),
                "VEND_IHUB_SUC",
            ),
            "vend_ihub_fail": scenario_df(
                matched,
                (
                    matched[status_column_excel]
                    .astype(str)
                    .str.lower()
                    .isin(["failed", "timed out"])
                )
                & (matched[status_column_db].astype(str).str.lower() == "failed")
                & (matched[ledger_status_col].astype(str).str.lower() == "yes"),
                "VEND_IHUB_FAIL",
            ),
            "vend_fail_ihub_succ": scenario_df(
                matched,
                (
                    matched[status_column_excel]
                    .astype(str)
                    .str.lower()
                    .isin(["failed", "timed out"])
                )
                & (matched[status_column_db].astype(str).str.lower() == "success")
                & (matched[ledger_status_col].astype(str).str.lower() == "yes"),
                "VEND_FAIL_IHUB_SUC",
            ),
            "vend_succ_ihub_fail": scenario_df(
                matched,
                (matched[status_column_excel].astype(str).str.lower() == "success")
                & (matched[status_column_db].astype(str).str.lower() == "failed")
                & (matched[ledger_status_col].astype(str).str.lower() == "yes"),
                "VEND_SUC_IHUB_FAIL",
            ),
            "ihub_initiate_vend_succes": scenario_df(
                matched,
                (matched[status_column_excel].astype(str).str.lower() == "success")
                & (
                    matched[status_column_db]
                    .astype(str)
                    .str.lower()
                    .isin(["initiated", "inprogress", "pending"])
                )
                & (matched[ledger_status_col].astype(str).str.lower() == "yes"),
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
                )
                & (matched[ledger_status_col].astype(str).str.lower() == "yes"),
                "VEND_FAIL_IHUB_INT",
            ),
        }

        # Success/failure counts
        matched_success_status = matched[
            (matched[status_column_db].astype(str).str.lower() == "success")
            & (matched[status_column_excel].astype(str).str.lower() == "success")
        ]

        success_count = matched_success_status.shape[0]
        matched_failed_status = matched[
            (matched[status_column_db].astype(str).str.lower() == "failed")
            & (
                matched[status_column_excel]
                .astype(str)
                .str.lower()
                .isin(["failed", "timed out"])
            )
        ]

        failed_count = matched_failed_status.shape[0]

        # Align and combine
        combine_keys = [
            "not_in_vendor",
            "not_in_portal",
            "vend_ihub_succ_not_in_ledger",
            "vend_fail_ihub_succ_not_in_ledger",
            "vend_succ_ihub_fail_not_in_ledger",
            "ihub_vend_fail_not_in_ledger",
            "ihub_initiate_vend_succes_not_in_ledger",
            "ihub_initiate_vend_fail_not_in_ledger",
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
            log("Filteration Ends")
            mapping = {
                "Total_Success_count": success_count,
                "Total_Failed_count": failed_count,
                "Excel_value_count": Excel_count,
                "HUB_Value_count": Hub_count,
                "VEND_IHUB_SUC": scenarios["vend_ihub_succ"],
                "VEND_IHUB_FAIL": scenarios["vend_ihub_fail"],
                "Hub_initiated_count": Hub_initiated_count,
                "Hub_success_count": Hub_success_count,
                "Hub_failed_count": Hub_failed_count,
                "Vendor_success_count": Vendor_success_count,
                "Vendor_failed_count": Vendor_failed_count,
                "Vendor_timeout_count": Vendor_timeout_count,
            }
        else:
            log("Filteration Ends")
            combined = pd.concat(non_empty_dfs, ignore_index=True)
            mapping = {
                "not_in_vendor": scenarios["not_in_vendor"],
                "combined": combined,
                "iti_Matched": iti_Matched,
                "not_in_Portal": scenarios["not_in_portal"],
                "VEND_IHUB_SUC-NIL": scenarios["vend_ihub_succ_not_in_ledger"],
                "VEND_FAIL_IHUB_SUC-NIL": scenarios[
                    "vend_fail_ihub_succ_not_in_ledger"
                ],
                "VEND_SUC_IHUB_FAIL-NIL": scenarios[
                    "vend_succ_ihub_fail_not_in_ledger"
                ],
                "IHUB_VEND_FAIL-NIL": scenarios["ihub_vend_fail_not_in_ledger"],
                "IHUB_INT_VEND_SUC-NIL": scenarios[
                    "ihub_initiate_vend_succes_not_in_ledger"
                ],
                "VEND_FAIL_IHUB_INT-NIL": scenarios[
                    "ihub_initiate_vend_fail_not_in_ledger"
                ],
                "VEND_IHUB_SUC": scenarios["vend_ihub_succ"],
                "VEND_IHUB_FAIL": scenarios["vend_ihub_fail"],
                "VEND_FAIL_IHUB_SUC": scenarios["vend_fail_ihub_succ"],
                "VEND_SUC_IHUB_FAIL": scenarios["vend_succ_ihub_fail"],
                "IHUB_INT_VEND_SUC": scenarios["ihub_initiate_vend_succes"],
                "VEND_FAIL_IHUB_INT": scenarios["ihub_initiate_vend_fail"],
                "Total_Success_count": success_count,
                "Total_Failed_count": failed_count,
                "Excel_value_count": Excel_count,
                "HUB_Value_count": Hub_count,
                "Hub_initiated_count": Hub_initiated_count,
                "Hub_success_count": Hub_success_count,
                "Hub_failed_count": Hub_failed_count,
                "Vendor_success_count": Vendor_success_count,
                "Vendor_failed_count": Vendor_failed_count,
                "Vendor_timeout_count": Vendor_timeout_count,
            }
        return mapping
    except Exception as e:
        logger.error(f"Error in unified_filtering_data: {str(e)}")


# ---------------------------------------------------------------------------------
# Filtering Function
def filtering_Data(df_db, df_excel, service_name, pan_nsdl_iti_df=None):

    # Use the unified filtering function with parameters matching the old logic
    if service_name == "ABHIBUS":
        print(df_excel["VENDOR_AMOUNT"])
        df_excel["VENDOR_AMOUNT"] = pd.to_numeric(
            df_excel["VENDOR_AMOUNT"], errors="coerce"
        )
        df_excel["Service Tax"] = pd.to_numeric(
            df_excel["Service Tax"], errors="coerce"
        )

        df_excel["VENDOR_AMOUNT"] = df_excel["VENDOR_AMOUNT"] + df_excel["Service Tax"]

        print(df_excel["VENDOR_AMOUNT"])

    if service_name in ["PASSPORT", "INSURANCE_OFFLINE"]:
        required_columns = [
            "CATEGORY",
            "VENDOR_DATE",
            "TENANT_ID",
            "IHUB_REFERENCE",
            "REFID",
            "IHUB_USERNAME",
            "VENDOR_AMOUNT",
            "HUB_AMOUNT",
            "COMMISSION_AMOUNT",
            "VENDOR_STATUS",
            "IHUB_MASTER_STATUS",
            f"{service_name}_STATUS",
            "SERVICE_DATE",
            "IHUB_LEDGER_STATUS",
            "TENANT_LEDGER_STATUS",
            "TRANSACTION_CREDIT",
            "TRANSACTION_DEBIT",
            "COMMISSION_CREDIT",
            "COMMISSION_REVERSAL",
        ]
    else:
        required_columns = [
            "CATEGORY",
            "VENDOR_DATE",
            "TENANT_ID",
            "IHUB_REFERENCE",
            "REFID",
            "IHUB_USERNAME",
            "BBPS_CATEGORY",
            "VENDOR_AMOUNT",
            "HUB_AMOUNT",
            "COMMISSION_AMOUNT",
            "VENDOR_STATUS",
            "IHUB_MASTER_STATUS",
            f"{service_name}_STATUS",
            "SERVICE_DATE",
            "IHUB_LEDGER_STATUS",
            "BILL_FETCH_STATUS",
            "TENANT_LEDGER_STATUS",
            "TRANSACTION_CREDIT",
            "TRANSACTION_DEBIT",
            "COMMISSION_CREDIT",
            "COMMISSION_REVERSAL",
        ]
    status_mapping_db = {
        0: "initiated",
        1: "success",
        2: "failed",
        3: "inprogress",
        4: "partial success",
    }
    # Call the unified function
    return unified_filtering_data(
        df_db,
        pan_nsdl_iti_df,
        df_excel,
        service_name,
        status_column_db="IHUB_MASTER_STATUS",
        status_column_excel="VENDOR_STATUS",
        required_columns=required_columns,
        ledger_status_col="IHUB_LEDGER_STATUS",
        status_mapping_db=status_mapping_db,
        logger_obj=logger,
    )
