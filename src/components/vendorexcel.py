import pandas as pd
from logger_config import logger
import traceback


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

        def safe_column_select(df, columns):
            existing_cols = [col for col in columns if col in df.columns]
            return df[existing_cols].copy()

        # Service-specific processing
        if service_name in ["DMT"]:
            statement_df["STATUS"] = (
                statement_df["STATUS"]
                .astype(str)
                .str.lower()
                .apply(lambda x: "failed" if "refunded" in x else "success")
            )

        if service_name in ["RECHARGE", "DMT"]:
            # Precompute frequently used values
            failed_count = statement_df[
                statement_df["STATUS"].str.strip().str.lower() == "failed"
            ].shape[0]
            credit_count = ledger_df[
                ledger_df["TYPE"].str.strip().str.lower() == "credit"
            ].shape[0]

            # Column renaming
            if "COMM/SHARE" in ledger_df.columns:
                ledger_df = ledger_df.rename(columns={"COMM/SHARE": "COMM"})
            if "NET COMMISSION" in statement_df.columns:
                statement_df = statement_df.rename(columns={"NET COMMISSION": "COMM"})

            # Date formatting
            for df in [ledger_df, statement_df]:
                if "DATE" in df.columns:
                    df["DATE"] = pd.to_datetime(
                        df["DATE"], errors="coerce"
                    ).dt.strftime("%Y-%m-%d")

            # Required columns
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

            # Clean TXNID columns
            statement_df["REFID"] = statement_df["REFID"].astype(str).str.strip()

            # Matching transactions
            matching_Trans_df = statement_df.merge(
                ledger_df[["TXNID", "TYPE"]], on="TXNID", how="inner"
            )
            matching_Trans_df = safe_column_select(matching_Trans_df, REQUIRED_COLUMNS)
            matched_count = matching_Trans_df.shape[0]

            # Records in ledger but not in statement
            ledger_txnids = ledger_df["TXNID"].astype(str).str.strip()
            statement_txnids = statement_df["TXNID"].astype(str).str.strip()

            not_in_statement_df = ledger_df[
                ~ledger_txnids.isin(statement_txnids)
            ].copy()
            not_in_statement_df = safe_column_select(
                not_in_statement_df, REQUIRED_COLUMNS
            )

            # Records in statement but not in ledger
            not_in_ledger_df = statement_df[
                ~statement_txnids.isin(ledger_txnids)
            ].copy()
            not_in_ledger_df = safe_column_select(not_in_ledger_df, REQUIRED_COLUMNS)

            # Extract REFUND_TXNID
            if "REFUND" in statement_df.columns:
                if service_name == "RECHARGE":
                    statement_df["REFUND_TXNID"] = statement_df["REFUND"].str.extract(
                        r"Txnid(\d+)", expand=False
                    )
                elif service_name == "DMT":
                    statement_df["REFUND_TXNID"] = statement_df[
                        "REFUNDTXNID"
                    ].str.strip()

            # Filter failed refunds
            failed_refunds = (
                statement_df.loc[
                    statement_df["STATUS"].str.lower().str.strip() == "failed"
                ].copy()
                if "STATUS" in statement_df.columns
                else pd.DataFrame(columns=statement_df.columns)
            )

            # Matching refunds
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
                    how="inner",
                    indicator=True,
                )
                .query('_merge == "both"')
                .drop(columns=["TXNID_y", "_merge"])
                .rename(columns={"TXNID_x": "TXNID"})
            )
            matching_refunds_df = safe_column_select(
                matching_refunds_df, REQUIRED_COLUMNS
            )

            # Mismatch handling
            if "REFUND_TXNID" in failed_refunds.columns:
                merged_df = failed_refunds.merge(
                    not_in_statement_df[["TXNID", "TYPE"]],
                    left_on="REFUND_TXNID",
                    right_on="TXNID",
                    how="outer",
                    indicator=True,
                )

                mismatch_statement_df = (
                    merged_df.query('_merge == "left_only"')
                    .drop(columns=["_merge", "TXNID_y"])
                    .rename(columns={"TXNID_x": "TXNID"})
                )

                mismatch_ledger_df = (
                    merged_df.query('_merge == "right_only"')
                    .drop(columns=["_merge", "TXNID_x"])
                    .rename(columns={"TXNID_y": "TXNID"})
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
                statement_df[~statement_df["SETTLED_ID"].isin(ledger_df["SNO"])]
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
            unmatched_statement = safe_column_select(
                unmatched_statement, REQUIRED_COLUMNS
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
        elif service_name in ["MATM"]:
            statement_df.columns = statement_df.columns.str.strip().str.upper()
            ledger_df.columns = ledger_df.columns.str.strip().str.upper()
            statement_df["AMOUNT"] = (
                statement_df["AMOUNT"]
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
            ledger_df["RRN"] = ledger_df["RRN"].apply(clean_rrn_value)
            ledger_df["AMOUNT"] = ledger_df["AMOUNT"].apply(clean_rrn_value)
            statement_df["UTR"] = statement_df["UTR"].astype(str).str.strip()

            # Remove rows with "NA" in RRN
            ledger_df = ledger_df[ledger_df["RRN"] != ""]

            # FIXED: This line was causing the error - removed incorrect usage
            # credit_count = ledger_df[ledger_df["RRN"].str.strip()].shape[0]  # WRONG
            credit_count = ledger_df.shape[
                0
            ]  # Count all remaining rows after filtering

            REQUIRED_COLUMNS = [
                "BCID",
                "AMOUNT_STATEMENT",
                "AMOUNT_LEDGER",
                "RRN",
                "DATE",
            ]

            # Rename amount columns
            if "AMOUNT" in ledger_df.columns:
                ledger_df = ledger_df.rename(
                    columns={"AMOUNT": "AMOUNT_LEDGER"}
                ).astype(str)
            if "AMOUNT" in statement_df.columns:
                statement_df = statement_df.rename(
                    columns={"AMOUNT": "AMOUNT_STATEMENT"}
                ).astype(str)

            # FIXED: Amount comparison - this was comparing entire columns incorrectly
            # amount_mismatch_rows = ledger_df[ledger_df["AMOUNT_LEDGER"] != statement_df["AMOUNT_STATEMENT"]]  # WRONG

            # Instead, merge first and then compare amounts
            merged_for_comparison = ledger_df.merge(
                statement_df, left_on="RRN", right_on="UTR", how="inner"
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
                ~ledger_df["RRN"].isin(statement_df["UTR"])
            ].copy()
            not_in_statement_df = safe_column_select(
                not_in_statement_df, REQUIRED_COLUMNS
            )

            not_in_ledger_df = (
                statement_df[~statement_df["UTR"].isin(ledger_df["RRN"])]
                .copy()
                .rename(columns={"UTR": "RRN"})
            )
            not_in_ledger_df = safe_column_select(not_in_ledger_df, REQUIRED_COLUMNS)

            # Merge for matched records
            merged_df = ledger_df.merge(
                statement_df, left_on="RRN", right_on="UTR", how="inner"
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
                    "message": "There is no Groupla Doopu..!🎉",
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
            amount_mismatch_rows.loc[:, "DATE"] = amount_mismatch_rows["TRANSACTION DATE_x"]
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
                    "message": "There is no Groupla Doopu..!🎉",
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
