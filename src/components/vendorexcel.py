import pandas as pd
from logger_config import logger
import traceback


def vendorexcel_reconciliation(
    service_name: str,
    vendor_ledger: pd.ExcelFile,
    vendor_statement: pd.ExcelFile,
) -> dict:  # 🔹 return type fixed
    try:
        logger.info(
            f"Starting vendor ledger reconciliation for service: {service_name}"
        )

        def safe_column_select(df, columns):
            existing_cols = [col for col in columns if col in df.columns]
            return df[existing_cols].copy()

        if service_name == "RECHARGE":
            ledger_df = pd.read_excel(vendor_ledger)
            statement_df = pd.read_excel(vendor_statement)
            ledger_count = ledger_df.shape[0]
            statement_count = statement_df.shape[0]
            failed_count = statement_df[
                statement_df["STATUS"].str.strip().str.lower() == "failed"
            ].shape[0]
            credit_count = ledger_df[
                ledger_df["TYPE"].str.strip().str.lower() == "credit"
            ].shape[0]
            ledger_df = ledger_df.rename(columns={"COMM/SHARE": "COMM"})
            if "DATE" in ledger_df.columns:
                ledger_df["DATE"] = pd.to_datetime(
                    ledger_df["DATE"], errors="coerce"
                ).dt.strftime("%Y-%m-%d")
            if "DATE" in statement_df.columns:
                statement_df["DATE"] = pd.to_datetime(
                    statement_df["DATE"], errors="coerce"
                ).dt.strftime("%Y-%m-%d")
            required_columns = [
                "TXNID",
                "USERNAME",
                "AMOUNT",
                "COMM",
                "TDS",
                "TYPE",
                "DATE",
                "REFID",
                "REFUND_TXNID",
            ]
            statement_df["REFID"] = statement_df["REFID"].astype(str).str.strip()
            matching_Trans_df = statement_df.merge(
                ledger_df[["TXNID", "TYPE"]], on="TXNID", how="inner"
            )
            matching_Trans_df = safe_column_select(matching_Trans_df, required_columns)
            matched_count = matching_Trans_df.shape[0]
            # 🔹 Records in ledger but not in statement
            not_in_statement_df = ledger_df[
                ~ledger_df["TXNID"]
                .astype(str)
                .str.strip()
                .isin(statement_df["TXNID"].astype(str).str.strip())
            ].copy()
            not_in_statement_df = safe_column_select(
                not_in_statement_df, required_columns
            )

            # 🔹 Records in statement but not in ledger
            not_in_ledger_df = statement_df[
                ~statement_df["TXNID"]
                .astype(str)
                .str.strip()
                .isin(ledger_df["TXNID"].astype(str).str.strip())
            ].copy()
            not_in_ledger_df = safe_column_select(not_in_ledger_df, required_columns)

            # 🔹 Extract only TxnID number from REFUND column
            if "REFUND" in statement_df.columns:
                statement_df["REFUND_TXNID"] = statement_df["REFUND"].str.extract(
                    r"Txnid(\d+)", expand=False
                )

            # 🔹 Filter only failed refunds
            if "STATUS" in statement_df.columns:
                failed_refunds = statement_df.loc[
                    statement_df["STATUS"].str.lower().str.strip() == "failed"
                ].copy()
            else:
                failed_refunds = pd.DataFrame(columns=statement_df.columns)

            # matching_refunds_df = failed_refunds[
            #     failed_refunds["REFUND_TXNID"].isin(
            #         not_in_statement_df["TXNID"].astype(str).str.strip()
            #     )
            # ].copy()
            not_in_statement_df["TXNID"] = (
                not_in_statement_df["TXNID"].astype(str).str.strip()
            )
            failed_refunds["REFUND_TXNID"] = (
                failed_refunds["REFUND_TXNID"].astype(str).str.strip()
            )
            matching_refunds_df = (
                failed_refunds.merge(
                    not_in_statement_df[["TXNID", "TYPE"]],
                    left_on="REFUND_TXNID",
                    right_on="TXNID",
                    how="inner",  # inner join is enough
                    indicator=True,
                )
                .query('_merge == "both"')  # keep only matches
                .drop(columns=["TXNID_y", "_merge"])
                .rename(columns={"TXNID_x": "TXNID"})
            )

            matching_refunds_df = safe_column_select(
                matching_refunds_df, required_columns
            )

            not_in_statement_df["TXNID"] = (
                not_in_statement_df["TXNID"].astype(str).str.strip()
            )
            if "REFUND_TXNID" in failed_refunds.columns:
                failed_refunds["REFUND_TXNID"] = (
                    failed_refunds["REFUND_TXNID"].astype(str).str.strip()
                )

            # 🔹 Left join to find unmatched refunds
            if "REFUND_TXNID" in failed_refunds.columns:
                not_matching_refunds_df = (
                    not_in_statement_df.merge(
                        failed_refunds[["REFUND_TXNID"]],
                        left_on="TXNID",
                        right_on="REFUND_TXNID",
                        how="left",
                        indicator=True,
                    )
                    .query('_merge == "left_only"')
                    .drop(columns=["REFUND_TXNID", "_merge"])
                )
            else:
                not_matching_refunds_df = not_in_statement_df.copy()

            not_matching_refunds_df = safe_column_select(
                not_matching_refunds_df, required_columns
            )

        else:
            logger.warning(
                f"Service {service_name} not supported for vendor ledger reconciliation."
            )
            return f"Service {service_name} not supported."

        logger.info("Vendor ledger reconciliation completed successfully.")
        if not_in_ledger_df.empty and not_matching_refunds_df.empty:
            return {
                "message": "There is no Groupla Doopu..!🎉",
                "matching_trans": matching_Trans_df,
                "matching_refunds": matching_refunds_df,
                "ledger_count": ledger_count,
                "statement_count": statement_count,
                "matched_trans_count": matched_count,
                "failed_trans_count": failed_count,
                "ledger_credit_count": credit_count,
            }
        else:
            return {
                "matching_trans": matching_Trans_df,
                "not_in_ledger": not_in_ledger_df,
                "matching_refunds": matching_refunds_df,
                "not_matching_refunds": not_matching_refunds_df,
                "ledger_count": ledger_count,
                "statement_count": statement_count,
                "matched_trans_count": matched_count,
                "failed_trans_count": failed_count,
                "ledger_credit_count": credit_count,
            }

    except Exception as e:
        logger.error(
            f"Vendor Ledger Reconciliation error: {str(e)}\n{traceback.format_exc()}"
        )
        return "Error processing vendor ledger reconciliation."
