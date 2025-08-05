"""
recon_utils.py - Shared reconciliation helper functions for DRY, maintainable code.
"""

import pandas as pd
from logger_config import logger


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
    df: pd.DataFrame, start_date, end_date, get_ebo_wallet_data_func
) -> pd.DataFrame:
    ebo_result = get_ebo_wallet_data_func(start_date, end_date)
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
