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
        ledger_df = pd.read_excel(vendor_ledger)
        statement_df = pd.read_excel(vendor_statement)
        ledger_count = ledger_df.shape[0]
        statement_count = statement_df.shape[0]

        def safe_column_select(df, columns):
            existing_cols = [col for col in columns if col in df.columns]
            return df[existing_cols].copy()

        if service_name in ["DMT"]:
            statement_df["STATUS"] = (
                statement_df["STATUS"]
                .astype(str)
                .str.lower()
                .apply(lambda x: "failed" if "refunded" in x else "success")
            )

        if service_name in ["RECHARGE", "DMT"]:

            failed_count = statement_df[
                statement_df["STATUS"].str.strip().str.lower() == "failed"
            ].shape[0]
            credit_count = ledger_df[
                ledger_df["TYPE"].str.strip().str.lower() == "credit"
            ].shape[0]
            if "COMM/SHARE" in ledger_df.columns:
                ledger_df = ledger_df.rename(columns={"COMM/SHARE": "COMM"})
            if "NET COMMISSION" in statement_df.columns:
                statement_df = statement_df.rename(columns={"NET COMMISSION": "COMM"})
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
                if service_name == "RECHARGE":
                    statement_df["REFUND_TXNID"] = statement_df["REFUND"].str.extract(
                        r"Txnid(\d+)", expand=False
                    )
                elif service_name == "DMT":
                    statement_df["REFUND_TXNID"] = statement_df[
                        "REFUNDTXNID"
                    ].str.strip()
            # 🔹 Filter only failed refunds
            if "STATUS" in statement_df.columns:
                failed_refunds = statement_df.loc[
                    statement_df["STATUS"].str.lower().str.strip() == "failed"
                ].copy()
            else:
                failed_refunds = pd.DataFrame(columns=statement_df.columns)
            print(failed_refunds)

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
            if "REFUND_TXNID" in failed_refunds.columns:
                merged_df = failed_refunds.merge(
                    not_in_statement_df[["TXNID", "TYPE"]],
                    left_on="REFUND_TXNID",
                    right_on="TXNID",
                    how="outer",
                    indicator=True,
                )
                # Rows only in failed_refunds → mismatch in statement
                mismatch_statement_df = (
                    merged_df.query('_merge == "left_only"')
                    .drop(columns=["_merge", "TXNID_y"])
                    .rename(columns={"TXNID_x": "TXNID"})
                )

                # Rows only in not_in_statement → mismatch in ledger
                mismatch_ledger_df = (
                    merged_df.query('_merge == "right_only"')
                    .drop(columns=["_merge", "TXNID_x"])
                    .rename(columns={"TXNID_y": "TXNID"})
                )
                print(mismatch_statement_df)
                print(mismatch_ledger_df)
            else:
                mismatch_statement_df = not_in_statement_df.copy()

            mismatch_ledger_df = safe_column_select(
                mismatch_ledger_df, required_columns
            )
            mismatch_statement_df = safe_column_select(
                mismatch_statement_df, required_columns
            )
            logger.info("Vendor ledger reconciliation completed successfully.")
            if (
                not_in_ledger_df.empty
                and mismatch_statement_df.empty
                and mismatch_ledger_df.empty
            ):
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
                    "mismatch_statement_refunds": mismatch_statement_df,
                    "mismatch_ledger_refunds": mismatch_ledger_df,
                    "ledger_count": ledger_count,
                    "statement_count": statement_count,
                    "matched_trans_count": matched_count,
                    "failed_trans_count": failed_count,
                    "ledger_credit_count": credit_count,
                }

        elif service_name in ["AEPS"]:
            statement_df.columns = statement_df.columns.str.strip().str.upper()
            ledger_df.columns = ledger_df.columns.str.strip().str.upper()
            
            # Count credit entries
            credit_count = ledger_df[ledger_df["TYPE"].str.strip().str.lower() == "credit"].shape[0]
            
            # Required columns definition
            REQUIRED_COLUMNS = [
                "SETTLED_ID", "COMMISSION_SNO", "SERIALNUMBER", "ACKNO", "UTR",
                "AMOUNT_LEDGER", "AMOUNT_STATEMENT", "COMMISSION_STATEMENT",
                "TYPE", "STATUS", "DATE"
            ]
            
            # Rename ledger amount column if needed
            if "AMOUNT" in ledger_df.columns:
                ledger_df = ledger_df.rename(columns={"AMOUNT": "AMOUNT_LEDGER"})
            
            # --- Step 1: Compare Settled ID with SNO ---
            # Group withdrawals by SETTLED_ID and sum amounts
            withdrawal_grouped = (
                statement_df.groupby("SETTLED_ID", as_index=False)["AMOUNT"]
                .sum()
                .rename(columns={"AMOUNT": "SUM_AMOUNT"})
            )
            # Merge grouped withdrawals with ledger
            merged_step1 = withdrawal_grouped.merge(
                ledger_df, left_on="SETTLED_ID", right_on="SNO", how="inner"
            )
            
            # Add comparison status
            amount_mismatch_rows = merged_step1[merged_step1["SUM_AMOUNT"] != merged_step1["AMOUNT_LEDGER"]]
            amount_mismatch_rows = safe_column_select(amount_mismatch_rows, REQUIRED_COLUMNS + ["SUM_AMOUNT"])
            print(amount_mismatch_rows)
            merged_step1 = merged_step1[merged_step1["SUM_AMOUNT"] == merged_step1["AMOUNT_LEDGER"]]
            # Attach original withdrawal details
            merged_step1_full = merged_step1.merge(
                statement_df.rename(columns={"AMOUNT": "AMOUNT_STATEMENT"}),
                on="SETTLED_ID",
                how="left"
            ).drop(columns=["SUM_AMOUNT"], errors="ignore")
            
            # --- Step 2: Identify unmatched ledger rows ---
            matched_snos = withdrawal_grouped["SETTLED_ID"]
            unmatched_ledger = ledger_df[~ledger_df["SNO"].isin(matched_snos)].copy()
            
            # Commission matches: ledger SNO exactly +1 of matched withdrawal SNO
            base_snos = merged_step1["SNO"] if "SNO" in merged_step1.columns else pd.Series([], dtype="int64")
            commission_match_mask = unmatched_ledger["SNO"].isin(base_snos + 1)
            commission_match = unmatched_ledger[commission_match_mask].copy().rename(columns={"SNO": "COMMISSION_SNO"})
            
            # Remaining unmatched ledger entries
            unmatched_ledger_final = unmatched_ledger[~commission_match_mask].copy()
            
            # --- Step 3: Unmatched withdrawals ---
            unmatched_statement = withdrawal_grouped[~withdrawal_grouped["SETTLED_ID"].isin(ledger_df["SNO"])]
            
            # --- Step 4: Merge commission matches with withdrawals ---
            # Determine SNO column for SNO_PLUS1 calculation
            sno_col = next((col for col in ["SNO", "SNO_x", "SNO_y"] if col in merged_step1_full.columns), None)
            
            if sno_col:
                merged_step1_full["SNO_PLUS1"] = pd.to_numeric(merged_step1_full[sno_col], errors="coerce") + 1
            else:
                merged_step1_full["SNO_PLUS1"] = pd.NA
            
            # Ensure numeric types for merging
            commission_match["COMMISSION_SNO"] = pd.to_numeric(commission_match["COMMISSION_SNO"], errors="coerce")
            merged_step1_full["SNO_PLUS1"] = pd.to_numeric(merged_step1_full["SNO_PLUS1"], errors="coerce")
            
            # Merge commission matches
            commission_merged = (
                commission_match.merge(
                    merged_step1_full,
                    left_on="COMMISSION_SNO",
                    right_on="SNO_PLUS1",
                    how="inner",
                    suffixes=("", "_STATEMENT")
                )
                .drop(columns=["SNO_PLUS1", "AMOUNT_LEDGER"], errors="ignore")
                .rename(columns={
                    "COMMISSION_y": "COMMISSION_STATEMENT",
                    "AMOUNT_LEDGER_STATEMENT": "AMOUNT_LEDGER"
                })
            )
            
            # Final column selection
            commission_merged = safe_column_select(commission_merged, REQUIRED_COLUMNS)
            unmatched_ledger_final = safe_column_select(unmatched_ledger_final, REQUIRED_COLUMNS)
            unmatched_statement = safe_column_select(unmatched_statement, REQUIRED_COLUMNS)
            
            # Count results
            matched_count = commission_merged.shape[0]
            failed_count = unmatched_statement.shape[0] + unmatched_ledger_final.shape[0]
            
            # Clean up numeric columns for display
            numeric_columns = ["SETTLED_ID", "SNO", "COMMISSION_SNO", "SNO_PLUS1", "SERIALNUMBER", "UTR"]
            for col in numeric_columns:
                if col in commission_merged.columns:
                    commission_merged[col] = (
                        commission_merged[col]
                        .fillna("")
                        .astype(str)
                        .str.replace(r"\.0$", "", regex=True)
                    )
            
            print(commission_merged)
            logger.info("Vendor ledger reconciliation completed successfully.")
            if unmatched_ledger_final.empty and unmatched_statement.empty and amount_mismatch_rows.empty:
                return {
                    "message": "There is no Groupla Doopu..!🎉",
                    "matching_trans": commission_merged,
                    "ledger_count": ledger_count,
                    "statement_count": statement_count,
                    "matched_trans_count": matched_count,
                    "failed_trans_count": failed_count,
                    "ledger_credit_count": credit_count,
                }
            else:
                return {
                    "matching_trans": commission_merged,
                    "not_in_statement": unmatched_ledger_final,
                    "not_in_ledger": unmatched_statement,
                    "ledger_count": ledger_count,
                    "amount_mismatch": amount_mismatch_rows,
                    "statement_count": statement_count,
                    "matched_trans_count": matched_count,
                    "failed_trans_count": failed_count,
                    "ledger_credit_count": credit_count,
                }

        else:
            logger.warning(
                f"Service {service_name} not supported for vendor ledger reconciliation."
            )
            return f"Service {service_name} not supported."

    except Exception as e:
        logger.error(
            f"Vendor Ledger Reconciliation error: {str(e)}\n{traceback.format_exc()}"
        )
        return "Error processing vendor ledger reconciliation."
