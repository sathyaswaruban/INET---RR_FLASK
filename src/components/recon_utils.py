"""
recon_utils.py - Shared reconciliation helper functions for DRY, maintainable code.
"""

import pandas as pd
from logger_config import logger
from db_connector import get_db_connection

DB_SERVICE_NAME_CONFIG = {
    "ASTRO": {"Db_service_name": "%Astrology%"},
    "ABHIBUS": {"Db_service_name": "%BusBooking%"},
    "BBPS": {"Db_service_name": "%BillPayment%"},
    "DMT": {"Db_service_name": "%Money Transfer%"},
    "INSURANCE_OFFLINE": {"Db_service_name": "%Insurance%"},
    "LIC": {"Db_service_name": "%LIC%"},
    "MANUAL_TB": {"Db_service_name": "%Manual%"},
    "MATM": {"Db_service_name": "%M-ATM%"},
    "MOVETOBANK": {"Db_service_name": "%MoveToBank%"},
    "RECHARGE": {"Db_service_name": "%All - Recharge%"},
    "AEPS": {"Db_service_name": "%AEPS%"},
    "PANUTI": {"Db_service_name": "%PAN-UTI%"},
    "PANNSDL": {"Db_service_name": "%Pan Internal%"},
    "PASSPORT": {"Db_service_name": "%Passport%"},
    "UPIQR": {"Db_service_name": "%UPI/QR%"},
}


def map_status_column(
    df: pd.DataFrame,
    status_col: str,
    status_mapping: dict,
    new_column: str,
    drop_original: bool = True,
) -> pd.DataFrame:
    if status_col in df.columns:
        df[new_column] = df[status_col].apply(lambda x: status_mapping.get(x, x))
        if drop_original:
            df.drop(columns=[status_col], inplace=True)
    return df


def map_tenant_id_column(
    df: pd.DataFrame, tenant_id_col: str = "TENANT_ID"
) -> pd.DataFrame:
    """Map tenant ID numbers to names if present."""
    tenant_Id_mapping = {
        1: "INET-CSC",
        2: "ITI-ESEVA",
        3: "UPCB",
    }
    if tenant_id_col in df.columns:
        df[tenant_id_col] = (
            df[tenant_id_col].map(tenant_Id_mapping).fillna(df[tenant_id_col])
        )
    return df


def merge_ebo_wallet_data(
    df: pd.DataFrame, start_date, end_date, service_name, get_ebo_wallet_data_func
) -> pd.DataFrame:
    db_service_name = DB_SERVICE_NAME_CONFIG[service_name]
    # print(db_service_name["Db_service_name"])
    ebo_result = get_ebo_wallet_data_func(
        start_date, end_date, db_service_name["Db_service_name"]
    )
    # if service_name == "PASSPORT":
       
            
    if ebo_result is not None and not ebo_result.empty:
        return pd.merge(
            df,
            ebo_result,
            how="left",
            left_on="IHUB_REFERENCE",
            right_on="IHubReferenceId",
            validate="one_to_one",
        )

    else:
        logger.warning("No data returned from EBO Wallet table.")
        return df
