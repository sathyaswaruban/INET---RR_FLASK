import pandas as pd
import numpy as np
import traceback
from logger_config import logger


def safe_column_select(df, columns):
    """Return a DataFrame with only the columns that exist in df."""
    existing_cols = [col for col in columns if col in df.columns]
    return df[existing_cols].copy()


def clean_rrn_value(value):
    """Clean RRN/Amount values: float->int, strip, handle NaN."""
    try:
        if pd.notna(value) and str(value).strip().lower() != "nan":
            return str(int(float(value)))
        return ""
    except (ValueError, TypeError):
        return str(value).strip() if pd.notna(value) else ""


def vendorexcel_reconciliation(
    service_name: str,
    vendor_ledger: pd.ExcelFile,
    vendor_statement: pd.ExcelFile,
) -> dict:
    try:
        logger.info(
            f"Starting vendor ledger reconciliation for service: {service_name}"
        )

        # Read dataframes
        ledger_df = pd.read_excel(vendor_ledger)
        statement_df = pd.read_excel(vendor_statement)
        ledger_count = ledger_df.shape[0]
        statement_count = statement_df.shape[0]

        # Service-specific processing

        # --- DMT specific status normalization ---
        """
        DMT Service Block
        - Normalizes status column to 'failed' or 'success'.
        """
        if service_name == "DMT":
            statement_df["STATUS"] = (
                statement_df["STATUS"]
                .astype(str)
                .str.lower()
                .apply(lambda x: "failed" if "refunded" in x else "success")
            )

        # --- RECHARGE & DMT reconciliation ---
        """
        RECHARGE & DMT Service Block
        - Handles matching, mismatches, and refund logic for recharge and DMT services.
        """
        if service_name in ["RECHARGE", "DMT"]:

            failed_count = statement_df[
                statement_df["STATUS"].str.strip().str.lower() == "failed"
            ].shape[0]
            credit_count = ledger_df[
                ledger_df["TYPE"].str.strip().str.lower() == "credit"
            ].shape[0]

            # Standardize commission columns
            ledger_df = (
                ledger_df.rename(columns={"COMM/SHARE": "COMM"})
                if "COMM/SHARE" in ledger_df.columns
                else ledger_df
            )
            statement_df = (
                statement_df.rename(columns={"NET COMMISSION": "COMM"})
                if "NET COMMISSION" in statement_df.columns
                else statement_df
            )

            # Date formatting
            for df in [ledger_df, statement_df]:
                if "DATE" in df.columns:
                    df["DATE"] = pd.to_datetime(
                        df["DATE"], errors="coerce"
                    ).dt.strftime("%Y-%m-%d")

            REQUIRED_COLUMNS = [
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
            matching_Trans_df = safe_column_select(matching_Trans_df, REQUIRED_COLUMNS)
            matched_count = matching_Trans_df.shape[0]

            ledger_txnids = ledger_df["TXNID"].astype(str).str.strip()
            statement_txnids = statement_df["TXNID"].astype(str).str.strip()

            not_in_statement_df = ledger_df[
                ~ledger_txnids.isin(statement_txnids)
            ].copy()
            not_in_statement_df = not_in_statement_df.rename(
                columns={"REFUNDTXNID": "REFUND_TXNID"}
            )
            not_in_statement_df = safe_column_select(
                not_in_statement_df, REQUIRED_COLUMNS
            )

            # Records in statement but not in ledger
            not_in_ledger_df = statement_df[
                ~statement_txnids.isin(ledger_txnids)
            ].copy()
            not_in_ledger_df = not_in_ledger_df.rename(
                columns={"REFUNDTXNID": "REFUND_TXNID"}
            )
            not_in_ledger_df = safe_column_select(not_in_ledger_df, REQUIRED_COLUMNS)

            # Extract REFUND_TXNID
            if "REFUND" in statement_df.columns and service_name == "RECHARGE":
                statement_df["REFUND_TXNID"] = statement_df["REFUND"].str.extract(
                    r"Txnid(\d+)", expand=False
                )
            elif "REFUNDTXNID" in statement_df.columns and service_name == "DMT":
                statement_df["REFUND_TXNID"] = statement_df["REFUNDTXNID"].str.strip()

            failed_refunds = (
                statement_df.loc[
                    statement_df["STATUS"].str.lower().str.strip() == "failed"
                ].copy()
                if "STATUS" in statement_df.columns
                else pd.DataFrame(columns=statement_df.columns)
            )

            not_in_statement_df["TXNID"] = (
                not_in_statement_df["TXNID"].astype(str).str.strip()
            )
            failed_refunds["REFUND_TXNID"] = (
                failed_refunds["REFUND_TXNID"].astype(str).str.strip()
            )
            # print(not_in_statement_df["TXNID"])
            matching_refunds_df = failed_refunds.merge(
                not_in_statement_df[["TXNID", "TYPE"]],
                left_on="REFUND_TXNID",
                right_on="TXNID",
                how="inner",
                indicator=True,
            )
            matching_refunds_df = (
                matching_refunds_df.query('_merge == "both"')
                .drop(columns=["TXNID_y", "_merge"])
                .rename(columns={"TXNID_x": "TXNID"})
            )
            matching_refunds_df = safe_column_select(
                matching_refunds_df, REQUIRED_COLUMNS
            )

            if "REFUND_TXNID" in failed_refunds.columns:
                merged_df = failed_refunds.merge(
                    not_in_statement_df[
                        [
                            "TXNID",
                            "TYPE",
                            "AMOUNT",
                            "COMM",
                            "TDS",
                            "DATE",
                            "REFID",
                            "REFUND_TXNID",
                        ]
                    ],
                    left_on="REFUND_TXNID",
                    right_on="TXNID",
                    how="outer",
                    indicator=True,
                )
                mismatch_statement_df = (
                    merged_df.query('_merge == "left_only"')
                    .drop(
                        columns=[
                            "_merge",
                            "TXNID_y",
                            "TDS_x",
                            "COMM_y",
                            "AMOUNT_y",
                            "DATE_y",
                            "REFID_y",
                            "REFUND_TXNID_y",
                        ]
                    )
                    .rename(
                        columns={
                            "TXNID_x": "TXNID",
                            "TDS_x": "TDS",
                            "COMM_x": "COMM",
                            "AMOUNT_x": "AMOUNT",
                            "DATE_x": "DATE",
                            "TYPE_x": "TYPE",
                            "REFID_x": "REFID",
                            "REFUND_TXNID_x": "REFUND_TXNID",
                        }
                    )
                )
                mismatch_ledger_df = (
                    merged_df.query('_merge == "right_only"')
                    .drop(
                        columns=[
                            "_merge",
                            "TXNID_x",
                            "TDS_x",
                            "COMM_x",
                            "AMOUNT_x",
                            "DATE_x",
                            "REFID_x",
                            "REFUND_TXNID_x",
                        ]
                    )
                    .rename(
                        columns={
                            "TXNID_y": "TXNID",
                            "TDS_y": "TDS",
                            "COMM_y": "COMM",
                            "AMOUNT_y": "AMOUNT",
                            "DATE_y": "DATE",
                            "TYPE_y": "TYPE",
                            "REFID_y": "REFID",
                            "REFUND_TXNID_y": "REFUND_TXNID",
                        }
                    )
                )
            else:
                mismatch_statement_df = not_in_statement_df.copy()
                mismatch_ledger_df = pd.DataFrame(columns=REQUIRED_COLUMNS)

            mismatch_ledger_df = safe_column_select(
                mismatch_ledger_df, REQUIRED_COLUMNS
            )
            mismatch_statement_df = safe_column_select(
                mismatch_statement_df, REQUIRED_COLUMNS
            )
            logger.info("Vendor ledger reconciliation completed successfully.")
            # Return results
            if (
                not_in_ledger_df.empty
                and mismatch_statement_df.empty
                and mismatch_ledger_df.empty
            ):
                return {
                    "message": "No Mismatch...",
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
                    "not_in_statement": mismatch_ledger_df,
                    "matching_refunds": matching_refunds_df,
                    "mismatch_statement_refunds": mismatch_statement_df,
                    # "mismatch_ledger_refunds": mismatch_ledger_df,
                    "ledger_count": ledger_count,
                    "statement_count": statement_count,
                    "matched_trans_count": matched_count,
                    "failed_trans_count": failed_count,
                    "ledger_credit_count": credit_count,
                }
        elif service_name in ["AEPS"]:
            """
            AEPS Service Block
            - Compares settled IDs, handles commission matches, and amount mismatches.
            """
            ledger_df = ledger_df[
                ~ledger_df["TXNTYPE"]
                .str.strip()
                .str.contains(
                    "settlement|Merchant Two Factor Authentication Charges|Ministatement",
                    case=False,
                    na=False,
                )
            ].copy()
            # Normalize column names
            statement_df.columns = statement_df.columns.str.strip().str.upper()
            ledger_df.columns = ledger_df.columns.str.strip().str.upper()

            # Count credit entries
            credit_count = ledger_df[
                ledger_df["TYPE"].str.strip().str.lower() == "credit"
            ].shape[0]

            # Required columns
            REQUIRED_COLUMNS = [
                "SETTLED_ID",
                "COMMISSION_SNO",
                "SERIALNUMBER",
                "ACKNO",
                "UTR",
                "AMOUNT_LEDGER",
                "AMOUNT_STATEMENT",
                "COMMISSION_STATEMENT",
                "TYPE",
                "STATUS",
                "DATE",
            ]

            # Rename amount column
            if "AMOUNT" in ledger_df.columns:
                ledger_df = ledger_df.rename(columns={"AMOUNT": "AMOUNT_LEDGER"})

            # Step 1: Compare Settled ID with SNO
            withdrawal_grouped = (
                statement_df.groupby("SETTLED_ID", as_index=False)["AMOUNT"]
                .sum()
                .rename(columns={"AMOUNT": "SUM_AMOUNT"})
            )
            print("Withdrawal Grouped Columns:", withdrawal_grouped.columns.tolist())

            merged_step1 = withdrawal_grouped.merge(
                ledger_df, left_on="SETTLED_ID", right_on="SNO", how="inner"
            )
            print("Merged Step 1 Columns:", merged_step1.columns.tolist())
            # Amount mismatch handling
            amount_mismatch_rows = merged_step1[
                merged_step1["SUM_AMOUNT"] != merged_step1["AMOUNT_LEDGER"]
            ]
            amount_mismatch_rows = amount_mismatch_rows.rename(
                columns={"COMMISSION": "COMMISSION_"}
            )
            amount_mismatch_rows = safe_column_select(
                amount_mismatch_rows, REQUIRED_COLUMNS + ["SUM_AMOUNT"]
            )

            merged_step1 = merged_step1[
                merged_step1["SUM_AMOUNT"] == merged_step1["AMOUNT_LEDGER"]
            ]

            merged_step1_full = merged_step1.merge(
                statement_df.rename(columns={"AMOUNT": "AMOUNT_STATEMENT"}),
                on="SETTLED_ID",
                how="left",
            ).drop(columns=["SUM_AMOUNT"], errors="ignore")

            # Step 2: Identify unmatched ledger rows
            matched_snos = withdrawal_grouped["SETTLED_ID"]
            unmatched_ledger = ledger_df[~ledger_df["SNO"].isin(matched_snos)].copy()

            # Commission matches
            base_snos = (
                merged_step1["SNO"]
                if "SNO" in merged_step1.columns
                else pd.Series([], dtype="int64")
            )
            commission_match_mask = unmatched_ledger["SNO"].isin(base_snos + 1)
            commission_match = (
                unmatched_ledger[commission_match_mask]
                .copy()
                .rename(columns={"SNO": "COMMISSION_SNO"})
            )

            unmatched_ledger_final = unmatched_ledger[~commission_match_mask].copy()
            unmatched_ledger_final = unmatched_ledger_final.rename(
                columns={"SNO": "SETTLED_ID"}
            )
            unmatched_ledger_final = safe_column_select(
                unmatched_ledger_final, REQUIRED_COLUMNS
            )
            # Step 3: Unmatched withdrawals
            unmatched_statement = (
                statement_df[
                    (~statement_df["SETTLED_ID"].isin(ledger_df["SNO"]))
                    & (statement_df["STATUS"].str.lower() != "failed")
                ]
                .rename(
                    columns={
                        "AMOUNT": "AMOUNT_STATEMENT",
                        "COMMISSION": "COMMISSION_STATEMENT",
                    }
                )
                .copy()
            )
            unmatched_statement = safe_column_select(
                unmatched_statement, REQUIRED_COLUMNS
            )
            # Step 4: Merge commission matches
            sno_col = next(
                (
                    col
                    for col in ["SNO", "SNO_x", "SNO_y"]
                    if col in merged_step1_full.columns
                ),
                None,
            )

            if sno_col:
                merged_step1_full["SNO_PLUS1"] = (
                    pd.to_numeric(merged_step1_full[sno_col], errors="coerce") + 1
                )
            else:
                merged_step1_full["SNO_PLUS1"] = pd.NA

            # Ensure numeric types
            commission_match["COMMISSION_SNO"] = pd.to_numeric(
                commission_match["COMMISSION_SNO"], errors="coerce"
            )
            merged_step1_full["SNO_PLUS1"] = pd.to_numeric(
                merged_step1_full["SNO_PLUS1"], errors="coerce"
            )

            # Merge commission matches
            commission_merged = (
                commission_match.merge(
                    merged_step1_full,
                    left_on="COMMISSION_SNO",
                    right_on="SNO_PLUS1",
                    how="inner",
                    suffixes=("", "_STATEMENT"),
                )
                .drop(columns=["SNO_PLUS1", "AMOUNT_LEDGER"], errors="ignore")
                .rename(
                    columns={
                        "COMMISSION_y": "COMMISSION_STATEMENT",
                        "AMOUNT_LEDGER_STATEMENT": "AMOUNT_LEDGER",
                    }
                )
            )

            # Final column selection
            commission_merged = safe_column_select(commission_merged, REQUIRED_COLUMNS)
            unmatched_ledger_final = safe_column_select(
                unmatched_ledger_final, REQUIRED_COLUMNS
            )
            # Count results
            matched_count = commission_merged.shape[0]
            failed_count = (
                unmatched_statement.shape[0] + unmatched_ledger_final.shape[0]
            )

            # Clean numeric columns
            numeric_columns = [
                "SETTLED_ID",
                "SNO",
                "COMMISSION_SNO",
                "SNO_PLUS1",
                "SERIALNUMBER",
                "UTR",
            ]
            for col in numeric_columns:
                if col in commission_merged.columns:
                    commission_merged[col] = (
                        commission_merged[col]
                        .fillna("")
                        .astype(str)
                        .str.replace(r"\.0$", "", regex=True)
                    )

            logger.info("Vendor ledger reconciliation completed successfully.")

            if (
                unmatched_ledger_final.empty
                and unmatched_statement.empty
                and amount_mismatch_rows.empty
            ):
                return {
                    "message": "No Mismatch..!",
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
        elif service_name in ["MATM"]:
            """
            MATM Service Block
            - Cleans RRN/amount columns, matches transactions, and finds mismatches.
            """
            statement_df.columns = statement_df.columns.str.strip().str.upper()
            ledger_df.columns = ledger_df.columns.str.strip().str.upper()
            ledger_df["AMOUNT"] = (
                ledger_df["AMOUNT"]
                .astype(str)  # Convert to string first
                .str.replace("₹", "", regex=False)  # Remove ₹ symbol
                .str.replace(" ", "", regex=False)  # Remove spaces
                .str.strip()  # Remove any remaining whitespace
            )

            # Handle RRN conversion properly
            def clean_rrn_value(value):
                try:
                    # Convert to float first to handle numbers, then to int to remove decimal
                    return (
                        str(int(float(value)))
                        if pd.notna(value) and str(value).strip() != "nan"
                        else ""
                    )
                except (ValueError, TypeError):
                    return str(value).strip()

            # Clean RRN and UTR columns
            statement_df["RRN"] = statement_df["RRN"].apply(clean_rrn_value)
            statement_df["AMOUNT"] = statement_df["AMOUNT"].apply(clean_rrn_value)
            ledger_df["UTR"] = ledger_df["UTR"].astype(str).str.strip()
            ledger_df["AMOUNT"] = ledger_df["AMOUNT"].apply(clean_rrn_value)

            # Remove rows with "NA" in RRN
            statement_df = statement_df[statement_df["RRN"] != ""]

            credit_count = ledger_df[
                ledger_df["CR/DR"].str.strip().str.lower() == "cr"
            ].shape[0]

            REQUIRED_COLUMNS = [
                "BCID",
                "AMOUNT_STATEMENT",
                "AMOUNT_LEDGER",
                "RRN",
                "DATE",
            ]

            # Rename amount columns
            if "AMOUNT" in statement_df.columns:
                statement_df = statement_df.rename(
                    columns={"AMOUNT": "AMOUNT_STATEMENT"}
                ).astype(str)
            if "AMOUNT" in ledger_df.columns:
                ledger_df = ledger_df.rename(
                    columns={"AMOUNT": "AMOUNT_LEDGER"}
                ).astype(str)

            # FIXED: Amount comparison - this was comparing entire columns incorrectly
            # amount_mismatch_rows = ledger_df[ledger_df["AMOUNT_LEDGER"] != statement_df["AMOUNT_STATEMENT"]]  # WRONG

            # Instead, merge first and then compare amounts
            merged_for_comparison = ledger_df.merge(
                statement_df, left_on="UTR", right_on="RRN", how="inner"
            )
            if not merged_for_comparison.empty:
                amount_mismatch_rows = merged_for_comparison[
                    merged_for_comparison["AMOUNT_LEDGER"]
                    != merged_for_comparison["AMOUNT_STATEMENT"]
                ]
            else:
                amount_mismatch_rows = pd.DataFrame()

            amount_mismatch_rows = safe_column_select(
                amount_mismatch_rows, REQUIRED_COLUMNS
            )

            # Find unmatched records
            not_in_statement_df = ledger_df[
                ~ledger_df["UTR"].isin(statement_df["RRN"])
            ].copy()
            not_in_statement_df = safe_column_select(
                not_in_statement_df, REQUIRED_COLUMNS
            )

            not_in_ledger_df = (
                statement_df[~statement_df["RRN"].isin(ledger_df["UTR"])]
                .copy()
                .rename(columns={"UTR": "RRN"})
            )
            not_in_ledger_df = safe_column_select(not_in_ledger_df, REQUIRED_COLUMNS)

            # Merge for matched records
            merged_df = ledger_df.merge(
                statement_df, left_on="UTR", right_on="RRN", how="inner"
            )
            merged_df = merged_df[
                merged_df["AMOUNT_LEDGER"] == merged_df["AMOUNT_STATEMENT"]
            ]

            if not merged_df.empty:
                matched_count = merged_df.shape[0]
                merged_df = safe_column_select(merged_df, REQUIRED_COLUMNS)
            else:
                matched_count = 0
                merged_df = pd.DataFrame(columns=REQUIRED_COLUMNS)

            failed_count = not_in_statement_df.shape[0] + not_in_ledger_df.shape[0]
            logger.info("Vendor ledger reconciliation completed successfully.")
            if (
                not_in_ledger_df.empty
                and not_in_statement_df.empty
                and amount_mismatch_rows.empty
            ):
                return {
                    "message": "No Mismatch...",
                    "matching_trans": merged_df,
                    "ledger_count": ledger_count,
                    "statement_count": statement_count,
                    "matched_trans_count": matched_count,
                    "failed_trans_count": failed_count,
                    "ledger_credit_count": credit_count,
                }
            else:
                return {
                    "matching_trans": merged_df,
                    "ledger_count": ledger_count,
                    "statement_count": statement_count,
                    "matched_trans_count": matched_count,
                    "failed_trans_count": failed_count,
                    "ledger_credit_count": credit_count,
                    "not_in_statement": not_in_statement_df,
                    "not_in_ledger": not_in_ledger_df,
                    "ledger_count": ledger_count,
                    "amount_mismatch": amount_mismatch_rows,
                }
        elif service_name in ["BBPS"]:
            """
            BBPS Service Block
            - Standardizes columns, matches transactions, and finds mismatches.
            """
            # Rename columns for consistency
            ledger_df = ledger_df.rename(
                columns={"Chq / Ref No.": "TRANS_REF_ID_LGR", "Debit": "AMOUNT_LEDGER"}
            )
            statement_df = statement_df.rename(
                columns={
                    "Transaction Ref ID": "TRANS_REF_ID_STMT",
                    "Transaction Amount(RS.)": "AMOUNT_STATEMENT",
                }
            )

            # Standardize column names
            statement_df.columns = statement_df.columns.str.strip().str.upper()
            ledger_df.columns = ledger_df.columns.str.strip().str.upper()

            # Filter ledger to only include transactions starting with "KM"
            ledger_df = ledger_df[
                ledger_df["TRANS_REF_ID_LGR"].str.startswith("KM", na=False)
            ]

            def clean_rrn_value(value):
                """
                Clean RRN/Amount values:
                - Convert float-like strings to int (remove decimals)
                - Return empty string for NaN/None
                - Strip spaces from strings
                """
                try:
                    if pd.notna(value) and str(value).strip().lower() != "nan":
                        return str(int(float(value)))
                    return ""
                except (ValueError, TypeError):
                    return str(value).strip() if pd.notna(value) else ""

            # Clean amount columns
            for col in ["AMOUNT_LEDGER", "AMOUNT_STATEMENT"]:
                if col in ledger_df.columns:
                    ledger_df[col] = ledger_df[col].apply(clean_rrn_value)
                if col in statement_df.columns:
                    statement_df[col] = statement_df[col].apply(clean_rrn_value)
            ledger_df["AMOUNT_LEDGER"] = ledger_df["AMOUNT_LEDGER"].astype(str)
            statement_df["AMOUNT_STATEMENT"] = statement_df["AMOUNT_STATEMENT"].astype(
                str
            )
            # Strip spaces from all string columns
            for df in [ledger_df, statement_df]:
                for col in df.columns:
                    if df[col].dtype == "object":
                        df[col] = df[col].apply(
                            lambda x: x.strip() if isinstance(x, str) else x
                        )

            REQUIRED_COLUMNS = [
                "TRANS_REF_ID",
                "AMOUNT_LEDGER",
                "AMOUNT_STATEMENT",
                "DATE",
            ]
            # Count credit transactions

            credit_trans = ledger_df[ledger_df["CREDIT"].notna()].copy()
            credit_trans = credit_trans.drop(columns=["AMOUNT_LEDGER"])
            credit_trans = credit_trans.rename(
                columns={
                    "CREDIT": "AMOUNT_LEDGER",
                    "TRANS_REF_ID_LGR": "TRANS_REF_ID",
                    "TRANSACTION DATE": "DATE",
                }
            )
            credit_count = credit_trans.shape[0]
            print("credit", credit_trans.columns.to_list())
            credit_trans = safe_column_select(credit_trans, REQUIRED_COLUMNS)
            # Define required columns

            # Find transactions not in statement
            not_in_statement_df = ledger_df[
                ~ledger_df["TRANS_REF_ID_LGR"].isin(statement_df["TRANS_REF_ID_STMT"])
            ].copy()
            not_in_statement_df = not_in_statement_df.rename(
                columns={"TRANS_REF_ID_LGR": "TRANS_REF_ID"}
            )
            # Add DATE column from ledger's Transaction Date
            not_in_statement_df["DATE"] = not_in_statement_df["TRANSACTION DATE"]
            not_in_statement_df = safe_column_select(
                not_in_statement_df, REQUIRED_COLUMNS
            )

            # Find transactions not in ledger
            not_in_ledger_df = statement_df[
                ~statement_df["TRANS_REF_ID_STMT"].isin(ledger_df["TRANS_REF_ID_LGR"])
            ].copy()
            not_in_ledger_df = not_in_ledger_df.rename(
                columns={"TRANS_REF_ID_STMT": "TRANS_REF_ID"}
            )
            # Add DATE column from statement's Transaction Date (renamed as requested)
            not_in_ledger_df["DATE"] = not_in_ledger_df["TRANSACTION DATE"]
            not_in_ledger_df = safe_column_select(not_in_ledger_df, REQUIRED_COLUMNS)

            # Merge dataframes
            merged_df = ledger_df.merge(
                statement_df,
                left_on="TRANS_REF_ID_LGR",
                right_on="TRANS_REF_ID_STMT",
                how="inner",
            )
            merged_df = merged_df.rename(columns={"TRANS_REF_ID_LGR": "TRANS_REF_ID"})

            # Find matching amounts
            amount_match_df = merged_df[
                merged_df["AMOUNT_LEDGER"] == merged_df["AMOUNT_STATEMENT"]
            ].copy()
            amount_mismatch_rows = merged_df[
                merged_df["AMOUNT_LEDGER"] != merged_df["AMOUNT_STATEMENT"]
            ].copy()

            # Add DATE column to amount_mismatch_rows
            amount_mismatch_rows.loc[:, "DATE"] = amount_mismatch_rows[
                "TRANSACTION DATE_x"
            ]
            amount_mismatch_rows = safe_column_select(
                amount_mismatch_rows, REQUIRED_COLUMNS
            )

            if not amount_match_df.empty:
                matched_count = amount_match_df.shape[0]
                # Add DATE column to matched transactions
                amount_match_df.loc[:, "DATE"] = amount_match_df["TRANSACTION DATE_x"]
                merged_df = safe_column_select(amount_match_df, REQUIRED_COLUMNS)
            else:
                matched_count = 0
                merged_df = pd.DataFrame(columns=REQUIRED_COLUMNS)

            failed_count = (
                not_in_statement_df.shape[0]
                + not_in_ledger_df.shape[0]
                + amount_mismatch_rows.shape[0]
            )
            logger.info("Vendor ledger reconciliation completed successfully.")
            if (
                not_in_ledger_df.empty
                and not_in_statement_df.empty
                and amount_mismatch_rows.empty
                and credit_trans.empty
            ):
                return {
                    "message": "No Mismatch...",
                    "matching_trans": merged_df,
                    "ledger_count": ledger_count,
                    "statement_count": statement_count,
                    "failed_count": failed_count,
                    "matched_count": matched_count,
                    "ledger_credit_count": credit_count,
                }
            else:
                return {
                    "matching_trans": merged_df,
                    "ledger_count": ledger_count,
                    "statement_count": statement_count,
                    "matched_trans_count": matched_count,
                    "failed_trans_count": failed_count,
                    "ledger_credit_count": credit_count,
                    "not_in_statement": not_in_statement_df,
                    "not_in_ledger": not_in_ledger_df,
                    "ledger_count": ledger_count,
                    "amount_mismatch": amount_mismatch_rows,
                    "credit_transactions_ledger": credit_trans,
                }
        elif service_name in ["LIC"]:
            """
            LIC Service Block
            - Standardizes columns, matches on TID and amount, and handles commission matching.
            """
            # === Standardize columns and read IDs as strings ===
            statement_df = pd.read_excel(
                vendor_statement, dtype={"IMWTID": str, "AMOUNT": str}
            )

            ledger_df = pd.read_excel(
                vendor_ledger,
                dtype={"TID": str, "REFERENCE TID": str, "DR": str, "CR": str},
            )

            # Standardize column names
            statement_df.columns = statement_df.columns.str.strip().str.upper()
            ledger_df.columns = ledger_df.columns.str.strip().str.upper()

            # Filter valid IMWTID rows
            statement_df = statement_df[
                statement_df["IMWTID"].notna()
                & (statement_df["IMWTID"].astype(str).str.strip() != "NA")
            ].copy()
            statement_count = statement_df.shape[0]

            # Rename columns
            statement_df = statement_df.rename(
                columns={"AMOUNT": "STMT_AMOUNT", "IMWTID": "STMT_TID"}
            )
            ledger_df = ledger_df.rename(
                columns={"DR": "LDG_AMOUNT", "TID": "LDG_TID", "CR": "COMM_AMT"}
            )

            # === Standardize TID columns as strings ===
            tid_columns = {
                "statement": ["STMT_TID"],
                "ledger": ["LDG_TID", "REFERENCE TID"],
            }

            for col in tid_columns["statement"]:
                statement_df[col] = statement_df[col].astype(str).str.strip()

            for col in tid_columns["ledger"]:
                ledger_df[col] = ledger_df[col].astype(str).str.strip()

            # === Clean and convert numeric columns ===
            numeric_columns = {
                "statement": ["STMT_AMOUNT"],
                "ledger": ["LDG_AMOUNT", "COMM_AMT"],
            }

            for col in numeric_columns["statement"]:
                statement_df[col] = pd.to_numeric(
                    statement_df[col].astype(str).str.strip(), errors="coerce"
                )

            for col in numeric_columns["ledger"]:
                ledger_df[col] = pd.to_numeric(
                    ledger_df[col].astype(str).str.strip(), errors="coerce"
                )

            # === Reconciliation logic ===
            REQUIRED_COLUMNS = [
                "LDG_TID",
                "COMMISSION_TID",
                "COMM_AMT",
                "LDG_AMOUNT",
                "STMT_AMOUNT",
                "DATE",
            ]

            # Find unmatched transactions
            not_in_ledger_df = statement_df[
                ~statement_df["STMT_TID"].isin(ledger_df["LDG_TID"])
            ].copy()
            not_in_ledger_df = not_in_ledger_df.rename(columns={"STMT_TID": "LDG_TID"})
            not_in_statement_df = ledger_df[
                ~ledger_df["LDG_TID"].isin(statement_df["STMT_TID"])
            ].copy()

            # Match on TID and amount
            matched_df = ledger_df.merge(
                statement_df,
                left_on="LDG_TID",
                right_on="STMT_TID",
                how="inner",
            )
            amount_matched_df = matched_df[
                matched_df["LDG_AMOUNT"] == matched_df["STMT_AMOUNT"]
            ]
            amount_matched_df = safe_column_select(amount_matched_df, REQUIRED_COLUMNS)
            # Amount mismatches
            amount_mismatch_rows = matched_df[
                matched_df["LDG_AMOUNT"] != matched_df["STMT_AMOUNT"]
            ].copy()
            amount_mismatch_rows = safe_column_select(
                amount_mismatch_rows, REQUIRED_COLUMNS
            )

            # Commission matching
            commission_match_df = not_in_statement_df[
                not_in_statement_df["LDG_TID"].isin(ledger_df["LDG_TID"])
            ].copy()

            # Ensure consistent data types for merging
            commission_match_df["REFERENCE TID"] = (
                commission_match_df["REFERENCE TID"].astype(str).str.strip()
            )
            amount_matched_df["LDG_TID"] = (
                amount_matched_df["LDG_TID"].astype(str).str.strip()
            )

            # Final unmatched statements

            # Merge commission transactions
            matched_trans_comm_df = amount_matched_df.merge(
                commission_match_df,
                left_on="LDG_TID",
                right_on="REFERENCE TID",
                how="inner",
            ).rename(
                columns={
                    "COMM_AMT_y": "COMM_AMT",
                    "LDG_AMOUNT_x": "LDG_AMOUNT",
                    "LDG_TID_x": "LDG_TID",
                    "LDG_TID_y": "COMMISSION_TID",
                }
            )
            amount_mismatch_comm_df = amount_mismatch_rows.merge(
                not_in_statement_df,
                left_on="LDG_TID",
                right_on="REFERENCE TID",
                how="inner",
            ).rename(
                columns={
                    "COMM_AMT_y": "COMM_AMT",
                    "LDG_AMOUNT_x": "LDG_AMOUNT",
                    "LDG_TID_x": "LDG_TID",
                    "LDG_TID_y": "COMMISSION_TID",
                }
            )
            amount_mismatch_comm_df = safe_column_select(
                amount_mismatch_comm_df, REQUIRED_COLUMNS
            )
            matched_trans_comm_df = safe_column_select(
                matched_trans_comm_df, REQUIRED_COLUMNS
            )
            not_in_ledger_df = safe_column_select(not_in_ledger_df, REQUIRED_COLUMNS)
            not_in_statement_final_df = not_in_statement_df[
                ~not_in_statement_df["LDG_TID"].isin(
                    matched_trans_comm_df["COMMISSION_TID"]
                )
            ].copy()
            not_in_statement_final_df = not_in_statement_final_df[
                ~not_in_statement_final_df["LDG_TID"].isin(
                    amount_mismatch_comm_df["COMMISSION_TID"]
                )
            ].copy()
            # print(not_in_statement_final_df.head(5))
            not_in_statement_final_df = safe_column_select(
                not_in_statement_final_df, REQUIRED_COLUMNS
            )

            # Count calculations
            matched_count = matched_trans_comm_df.shape[0]
            failed_count = (
                not_in_ledger_df.shape[0]
                + not_in_statement_final_df.shape[0]
                + amount_mismatch_rows.shape[0]
            )
            credit_count = ledger_df[
                ledger_df["COMM_AMT"].notna() & (ledger_df["COMM_AMT"] != 0)
            ].shape[0]
            ledger_count = ledger_df.shape[0]

            logger.info("Vendor ledger reconciliation completed successfully.")
            # Prepare response
            result_data = {
                "matching_trans": matched_trans_comm_df,
                "ledger_count": ledger_count,
                "statement_count": statement_count,
                "matched_trans_count": matched_count,
                "failed_trans_count": failed_count,
                "ledger_credit_count": credit_count,
            }

            # Add mismatch data only if present
            if not (
                amount_mismatch_rows.empty
                and not_in_ledger_df.empty
                and not_in_statement_final_df.empty
            ):
                result_data.update(
                    {
                        "not_in_statement": not_in_statement_final_df,
                        "not_in_ledger": not_in_ledger_df,
                        "amount_mismatch": amount_mismatch_comm_df,
                    }
                )
            else:
                result_data["message"] = "No Mismatch..."

            return result_data
        elif service_name in ["PANUTI"]:
            """
            PANUTI Service Block
            - Cleans and matches reference numbers, handles failed rows, and finds unmatched records.
            """
            statement_df.columns = statement_df.columns.str.strip().str.upper()
            ledger_df.columns = ledger_df.columns.str.strip().str.upper()
            statement_df = statement_df.rename(
                columns={
                    "REFERENCE NO": "STMT_REFERENCE_NO",
                    "RES AMOUNT": "AMOUNT_STATEMENT",
                    "TRANS DATE": "STATEMENT_DATE",
                }
            )
            ledger_df = ledger_df.rename(
                columns={
                    "REFERENCE NO": "LDG_REFERENCE_NO",
                    "UTIITSL AMOUNT": "AMOUNT_LEDGER",
                    "LOT DATE": "LEDGER_DATE",
                }
            )
            failed_rows = (
                statement_df["PAYMENT STATUS"]
                == "Payment Refunded due to Incomplete Application"
            )
            failed_df = statement_df[failed_rows].copy()
            cleaned_statement_df = statement_df[~failed_rows].copy()
            REQUIRED_COLUMNS = [
                "LDG_REFERENCE_NO",
                "STMT_REFERENCE_NO",
                "AMOUNT_LEDGER",
                "AMOUNT_STATEMENT",
                "LEDGER_DATE",
                "STATEMENT_DATE",
            ]
            cleaned_statement_df["STMT_REFERENCE_NO"] = (
                cleaned_statement_df["STMT_REFERENCE_NO"].astype(str).str.strip()
            )
            ledger_df["LDG_REFERENCE_NO"] = (
                ledger_df["LDG_REFERENCE_NO"].astype(str).str.strip()
            )
            not_in_ledger_df = cleaned_statement_df[
                ~cleaned_statement_df["STMT_REFERENCE_NO"].isin(
                    ledger_df["LDG_REFERENCE_NO"]
                )
            ].copy()
            not_in_ledger_df = safe_column_select(not_in_ledger_df, REQUIRED_COLUMNS)
            not_in_statement_df = ledger_df[
                ~ledger_df["LDG_REFERENCE_NO"].isin(
                    cleaned_statement_df["STMT_REFERENCE_NO"]
                )
            ].copy()
            not_in_statement_df = safe_column_select(
                not_in_statement_df, REQUIRED_COLUMNS
            )
            merged_df = ledger_df.merge(
                cleaned_statement_df,
                left_on="LDG_REFERENCE_NO",
                right_on="STMT_REFERENCE_NO",
                how="inner",
            )
            matched_df = safe_column_select(merged_df, REQUIRED_COLUMNS)
            matched_count = matched_df.shape[0]
            failed_count = (
                not_in_ledger_df.shape[0]
                + not_in_statement_df.shape[0]
                + failed_df.shape[0]
            )
            logger.info("Vendor ledger reconciliation completed successfully.")
            # Prepare response
            result_data = {
                "matching_trans": matched_df,
                "ledger_count": ledger_count,
                "statement_count": statement_count,
                "matched_trans_count": matched_count,
                "failed_trans_count": failed_count,
            }
            if not (not_in_ledger_df.empty and not_in_statement_df.empty):
                result_data.update(
                    {
                        "not_in_statement": not_in_statement_df,
                        "not_in_ledger": not_in_ledger_df,
                    }
                )
            else:
                result_data["message"] = "No Mismatch..."

            return result_data
        elif service_name in ["ABHIBUS"]:
            """
            ABHIBUS Service Block
            - Calculates final amounts, groups by date, and flags matches/mismatches.
            """
            statement_df.columns = statement_df.columns.str.strip().str.upper()
            ledger_df.columns = ledger_df.columns.str.strip().str.upper()

            statement_df["BOOKED DATE"] = pd.to_datetime(
                statement_df["BOOKED DATE"], format="%d-%m-%Y %H:%M"
            )
            ledger_df["BALANCE DATE"] = pd.to_datetime(
                ledger_df["BALANCE DATE"], format="%d-%m-%Y"
            )
            # ledger_df["USED AMOUNT"] = ledger_df["USED AMOUNT"].abs()
            # --- Step 1: Calculate FINAL_AMOUNT in statement_df ---
            statement_df["FINAL_AMOUNT"] = (
                statement_df["TICKET AMOUNT"] + statement_df["SERVICE TAX"]
            ) - (statement_df["COMM."] - statement_df["COMM TDS"])

            # --- Step 2: Extract only date (ignore time) ---
            statement_df["BOOKED_DATE_ONLY"] = statement_df["BOOKED DATE"].dt.date
            ledger_df["BALANCE_DATE_ONLY"] = ledger_df["BALANCE DATE"].dt.date

            grouped = statement_df.groupby(
                ["BOOKED_DATE_ONLY", "STATUS"], as_index=False
            )["FINAL_AMOUNT"].sum()

            # Pivot so SUCCESS and FAILED become columns
            pivoted = grouped.pivot_table(
                index="BOOKED_DATE_ONLY",
                columns="STATUS",
                values="FINAL_AMOUNT",
                fill_value=0,
            ).reset_index()

            # Calculate net amount (Success - Failed)
            pivoted["AMOUNT_STATEMENT"] = pivoted.get("success", 0) - pivoted.get(
                "failed", 0
            )

            # Keep only date + net amount for comparison
            group_by_date = pivoted[["BOOKED_DATE_ONLY", "AMOUNT_STATEMENT"]]
            group_by_date.columns = ["DATE", "AMOUNT_STATEMENT"]

            # print(group_by_date[["DATE","AMOUNT_STATEMENT"]])

            ledger_group = (
                ledger_df.assign(USED_AMOUNT_ABS=ledger_df["USED AMOUNT"])
                .groupby("BALANCE_DATE_ONLY", as_index=False)["USED_AMOUNT_ABS"]
                .sum()
            )
            ledger_group.columns = ["DATE", "AMOUNT_LEDGER"]

            # --- Step 4: Merge grouped totals for comparison ---
            comparison_df = pd.merge(
                group_by_date, ledger_group, on="DATE", how="outer"
            )

            # --- Step 4.1: Fill missing values with 0 ---
            comparison_df = comparison_df.fillna(0)

            # --- Step 5: Flag match/mismatch with tolerance ---
            comparison_df["MAPPING_STATUS"] = np.where(
                np.isclose(
                    comparison_df["AMOUNT_STATEMENT"],
                    comparison_df["AMOUNT_LEDGER"],
                    rtol=1e-5,
                    atol=1e-5,
                ),
                "MATCHED",
                "MISMATCHED",
            )

            # print(comparison_df[["DATE", "AMOUNT_STATEMENT", "AMOUNT_LEDGER", "MAPPING_STATUS"]])
            # --- Step 6: Attach STATUS back to original statement_df ---
            statement_with_status = statement_df.merge(
                comparison_df[
                    ["DATE", "MAPPING_STATUS", "AMOUNT_STATEMENT", "AMOUNT_LEDGER"]
                ],
                left_on="BOOKED_DATE_ONLY",
                right_on="DATE",
                how="left",
            )
            REQUIRED_COLUMNS = [
                "TKT. NUMBER",
                "AMOUNT_LEDGER",
                "AMOUNT_STATEMENT",
                "BOOKED DATE",
                "TICKET AMOUNT",
                "SERVICE TAX",
                "COMM.",
                "COMM TDS",
                "FINAL_AMOUNT",
                "MAPPING_STATUS",
            ]
            # --- Step 7: Split into matched/mismatched dataframes with original rows ---
            matched_df = statement_with_status[
                statement_with_status["MAPPING_STATUS"] == "MATCHED"
            ]
            mismatched_df = statement_with_status[
                statement_with_status["MAPPING_STATUS"] == "MISMATCHED"
            ]
            matched_df = safe_column_select(matched_df, REQUIRED_COLUMNS)
            mismatched_df = safe_column_select(mismatched_df, REQUIRED_COLUMNS)
            credit_count = ledger_df[ledger_df["USED AMOUNT"] < 0].shape[0]
            result_data = {
                "ledger_count": ledger_count,
                "statement_count": statement_count,
                "credit_count": credit_count,
                "matched_trans_count": matched_df.shape[0],
                "failed_trans_count": mismatched_df.shape[0],
                "matching_trans": matched_df,
            }
            if not (mismatched_df.empty):
                result_data.update(
                    {
                        "amount_mismatch": mismatched_df,
                    }
                )
            else:
                result_data["message"] = "No Mismatch..."
            return result_data

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
