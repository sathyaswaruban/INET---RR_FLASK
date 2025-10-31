import pandas as pd
from db_connector import get_db_connection
from logger_config import logger
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Any, Optional
from sqlalchemy import text
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from sqlalchemy.exc import OperationalError, DatabaseError
from components.recon_utils import map_status_column, map_tenant_id_column

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

    # Add other service configurations here


def up_service_selection(
    start_date: str, end_date: str, service_name: str, df_excel: pd.DataFrame
) -> Any:
    """Handle up service selection and processing."""
    logger.info(f"Entering Reconciliation for {service_name} Service")

    # Validate service name
    if service_name not in SERVICE_CONFIGS:
        logger.warning("OutwardService function selection Error")
        return "Service Name Error..!"

    service_config = SERVICE_CONFIGS[service_name]

    try:

        hub_data = service_config["service_func"](start_date, end_date, service_name)
        return filtering_Data(hub_data, df_excel, service_name)

    except Exception as e:
        logger.error(f"Error processing {service_name} service: {str(e)}")
        return f"Error processing {service_name} service"


def filtering_Data(df_db, df_excel, service_name):
    try:
        logger.info(f"Filteration Starts for {service_name} service")
        mapping = None
        message = None
        Excel_count = len(df_excel)
        Hub_count = None
        df_db["SERVICE_DATE"] = pd.to_datetime(
            df_db["SERVICE_DATE"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        df_excel["VENDOR_DATE"] = pd.to_datetime(
            df_excel["VENDOR_DATE"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        # tenant_data["TENANT_STATUS"] = tenant_data["TENANT_STATUS"].apply(
        #     lambda x: status_mapping.get(x, x)
        # )

        # function to select only required cols and make it as df
        def safe_column_select(df, columns):
            existing_cols = [col for col in columns if col in df.columns]
            return df[existing_cols].copy()

        # Required columns that to be sent as result to UI
        if service_name == "MANUAL_TB":
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
            df_db["VENDOR_REFERENCE"] = (
                df_db["VENDOR_REFERENCE"]
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
            )
            df_excel["REFID"] = (
                df_excel["REFID"].str.strip().str.replace(r"\s+", " ", regex=True)
            )

            df_db = map_status_column(
                df_db,
                "service_status",
                status_mapping,
                new_column=f"{service_name}_STATUS",
                drop_original=True,
            )
            df_db = map_tenant_id_column(df_db)
            required_columns = [
                "IHUB_USERNAME",
                "NAME",
                "BANK_NAME",
                "REFID",
                "ACC_NO",
                "UTR_NO",
                "AMOUNT",
                f"{service_name}_STATUS",
                "SERVICE_DATE",
            ]
        else:
            required_columns = [
                "EBO_ID",
                "USERNAME",
                "VLE_ID",
                "REFID",
                "SERVICE_CODE",
                "APPLICATION_COUNT",
                "RATE",
                "TOTAL_AMOUNT",
                "JENSEVA_TYPE",
                "VENDOR_DATE",
                "SERVICE_DATE",
                "SUB_DISTRICT",
                "VILLAGE",
            ]

        matched = df_db.merge(
            df_excel, left_on="VENDOR_REFERENCE", right_on="REFID", how="inner"
        ).copy()
        matched["CATEGORY"] = "MATCHED"
        matched = safe_column_select(matched, required_columns)
        # print(matched.head(5))
        # 1 Filtering Data initiated in IHUB portal and not in Vendor Xl
        not_in_vendor = df_db[~df_db["VENDOR_REFERENCE"].isin(df_excel["REFID"])].copy()
        not_in_vendor["CATEGORY"] = "NOT_IN_VENDOR"
        not_in_vendor = not_in_vendor.rename(columns={"VENDOR_REFERENCE": "REFID"})
        not_in_vendor = safe_column_select(not_in_vendor, required_columns)
        # 2. Filtering Data Present in Vendor XL but Not in Ihub Portal
        not_in_portal_1 = df_excel[
            ~df_excel["REFID"].isin(df_db["VENDOR_REFERENCE"])
        ].copy()
        not_in_portal_app_ids = not_in_portal_1["REFID"]

        if not not_in_portal_app_ids.dropna().empty:
            service = SERVICE_CONFIGS[service_name]
            query = service["Date_diff_query"]
            params = {"app_ids": tuple(not_in_portal_app_ids)}
            not_in_portal_1_db_check = execute_sql_with_retry(query, params=params)
            in_portal_date_diff_df = not_in_portal_1_db_check.merge(
                df_excel, left_on="VENDOR_REFERENCE", right_on="REFID", how="inner"
            ).copy()
            in_portal_date_diff_df["CATEGORY"] = "IN_PORTAL_DIFF_DATE"
            not_in_portal = not_in_portal_1[
                ~not_in_portal_1["REFID"].isin(
                    in_portal_date_diff_df["VENDOR_REFERENCE"]
                )
            ].copy()
            in_portal_date_diff_df["SERVICE_DATE"] = pd.to_datetime(
                in_portal_date_diff_df["SERVICE_DATE"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
            in_portal_date_diff_df = safe_column_select(
                in_portal_date_diff_df, required_columns
            )
            not_in_portal["CATEGORY"] = "NOT_IN_PORTAL"
            not_in_portal = safe_column_select(not_in_portal, required_columns)
            success_count = matched.shape[0] + in_portal_date_diff_df.shape[0]
            Hub_count = df_db.shape[0] + in_portal_date_diff_df.shape[0]
        else:
            if not_in_portal_1.empty and not_in_vendor.empty:
                message = "Hurray..! There is no mismatch Data Found."

            not_in_portal = not_in_portal_1
            Hub_count = df_db.shape[0]
            success_count = matched.shape[0]
            in_portal_date_diff_df = None

        # 4. Filtering Data that matches in both Ihub Portal and Vendor Xl as : Matched
        # 5. Filtering Data that Mismatched in both Ihub Portal and Vendor Xl as : Mismatched
        failed_count = not_in_portal.shape[0] + not_in_vendor.shape[0]

        logger.info("Filteration Ends")
        mapping = {
            "not_in_vendor": not_in_vendor,
            "not_in_Portal": not_in_portal,
            "In_portal_date_diff": in_portal_date_diff_df,
            "matched": matched,
            "Excel_value_count": Excel_count,
            "HUB_Value_count": Hub_count,
            "Total_Success_count": success_count,
            "Total_Failed_count": failed_count,
            "message": message,
        }
        return mapping

    except Exception as e:
        logger.warning("Error inside Filtering Function: %s", e)
        print(f"Error inside Filtering Function: {e}")
        message = "Error in Filteration"
        return message


def service_function(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()
    service = SERVICE_CONFIGS[service_name]
    query = service["main_query"]
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

        df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
        result = df_db

    except SQLAlchemyError as e:
        logger.error(f"Database error in UPIQR_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in UPIQR_Service(): {e}")

    return result


# Define service configurations
SERVICE_CONFIGS = {
    "SULTANPURSCA": {
        "main_query": text(
            """ SELECT u.UserName AS EBO_ID,ed.Name AS USERNAME,udt.VleId AS VLE_ID,udt.ApplicationId AS VENDOR_REFERENCE,
                            uds.Code AS SERVICE_CODE,udt.Count AS APPLICATION_COUNT,
                            udt.Commission AS RATE,udt.Amount AS TOTAL_AMOUNT,
                            CASE 
                                WHEN u.jansevaType = 1 THEN 'Jenseva Kendra'
                                WHEN u.jansevaType = 2 THEN 'Panchayat Sahayak'
                                ELSE 'EBO' 
                            END AS JENSEVA_TYPE,
                            udt.CreationTs AS SERVICE_DATE,
                            sd.Name AS SUB_DISTRICT,
                            ed.VillageName AS VILLAGE
                            FROM tenantinetcsc.UpeDistrictTransaction udt
                            JOIN (
                                SELECT MIN(id) AS min_id
                                FROM tenantinetcsc.UpeDistrictTransaction
                                WHERE DATE(CreationTs) BETWEEN :start_date AND :end_date
                                AND UpeDistrictServiceId NOT IN (1)
                                GROUP BY ApplicationId
                            ) uniq ON udt.id = uniq.min_id
                            LEFT JOIN tenantinetcsc.EboDetail ed ON ed.id = udt.EboDetailId 
                            LEFT JOIN tenantinetcsc.`User` u ON ed.UserId = u.id
                            LEFT JOIN tenantinetcsc.UpeDistrictService uds ON uds.id = udt.UpeDistrictServiceId
                            LEFT JOIN tenantinetcsc.SubDistrict sd ON sd.id = ed.SubDistrictId
                            ORDER BY SERVICE_DATE ASC;
                        """
        ),
        "Date_diff_query": text(
            """SELECT u.UserName AS EBO_ID,ed.Name AS USERNAME,udt.VleId AS VLE_ID,udt.ApplicationId AS VENDOR_REFERENCE,
                                uds.Code AS SERVICE_CODE,udt.Count AS APPLICATION_COUNT,udt.Commission AS RATE,udt.Amount AS TOTAL_AMOUNT,
                                CASE 
                                    WHEN u.jansevaType = 1 THEN 'Jenseva Kendra'
                                    WHEN u.jansevaType = 2 THEN 'Panchayat Sahayak'
                                    ELSE 'EBO' 
                                END AS JENSEVA_TYPE,
                                udt.CreationTs AS SERVICE_DATE,
                                sd.Name AS SUB_DISTRICT,
                                    ed.VillageName AS VILLAGE
                                FROM tenantinetcsc.UpeDistrictTransaction udt
                                JOIN (
                                    SELECT ApplicationId, MAX(CreationTs) AS max_ts
                                    FROM tenantinetcsc.UpeDistrictTransaction
                                    WHERE UpeDistrictServiceId NOT IN (1)
                                    AND ApplicationId IN :app_ids
                                    GROUP BY ApplicationId
                                ) AS latest ON udt.ApplicationId = latest.ApplicationId AND udt.CreationTs = latest.max_ts
                                LEFT JOIN tenantinetcsc.EboDetail ed ON ed.id = udt.EboDetailId
                                LEFT JOIN tenantinetcsc.`User` u ON ed.UserId = u.id
                                LEFT JOIN tenantinetcsc.UpeDistrictService uds ON uds.id = udt.UpeDistrictServiceId
                                LEFT JOIN tenantinetcsc.SubDistrict sd ON sd.id = ed.SubDistrictId;
                            """
        ),
        "service_func": service_function,
    },
    "SULTANPUR_IS": {
        "main_query": text(
            """ SELECT u.UserName AS EBO_ID,ed.Name AS USERNAME,udt.VleId AS VLE_ID,udt.QuotaId AS VENDOR_REFERENCE,
                            uds.Code AS SERVICE_CODE,udt.Count AS APPLICATION_COUNT,udt.Commission AS RATE,udt.Amount AS TOTAL_AMOUNT,
                            CASE 
                            WHEN u.jansevaType = 1 THEN 'Jenseva Kendra'
                            WHEN u.jansevaType = 2 THEN 'Panchayat Sahayak'
                            ELSE 'EBO' 
                            END AS JENSEVA_TYPE,
                            udt.CreationTs AS SERVICE_DATE,sd.Name AS SUB_DISTRICT,ed.VillageName AS VILLAGE
                            FROM tenantinetcsc.UpeDistrictTransaction udt
                            JOIN (
                                SELECT 
                            QuotaId, 
                            MAX(CreationTs) AS max_ts
                            FROM tenantinetcsc.UpeDistrictTransaction
                            WHERE 
                                DATE(CreationTs) BETWEEN :start_date AND :end_date
                                AND UpeDistrictServiceId IN (1)
                                GROUP BY QuotaId
                            ) AS latest ON udt.QuotaId = latest.QuotaId AND udt.CreationTs = latest.max_ts
                            LEFT JOIN tenantinetcsc.EboDetail ed ON ed.id = udt.EboDetailId
                            LEFT JOIN tenantinetcsc.`User` u ON ed.UserId = u.id
                            LEFT JOIN tenantinetcsc.UpeDistrictService uds ON uds.id = udt.UpeDistrictServiceId
                            LEFT JOIN tenantinetcsc.SubDistrict sd ON sd.id = ed.SubDistrictId
                            WHERE udt.UpeDistrictServiceId IN (1);
                    """
        ),
        "Date_diff_query": text(
            """SELECT u.UserName AS EBO_ID,ed.Name AS USERNAME,udt.VleId AS VLE_ID,udt.QuotaId AS VENDOR_REFERENCE,
                                uds.Code AS SERVICE_CODE,udt.Count AS APPLICATION_COUNT,udt.Commission AS RATE,udt.Amount AS TOTAL_AMOUNT,
                                CASE 
                                    WHEN u.jansevaType = 1 THEN 'Jenseva Kendra'
                                    WHEN u.jansevaType = 2 THEN 'Panchayat Sahayak'
                                    ELSE 'EBO' 
                                END AS JENSEVA_TYPE,
                                udt.CreationTs AS SERVICE_DATE,
                                sd.Name AS SUB_DISTRICT,
                                ed.VillageName AS VILLAGE
                                FROM tenantinetcsc.UpeDistrictTransaction udt
                                JOIN (
                                SELECT 
                                    ApplicationId,
                                    MAX(CreationTs) AS max_ts
                                FROM tenantinetcsc.UpeDistrictTransaction
                                WHERE ApplicationId IN :app_ids AND UpeDistrictServiceId IN (1)
                                GROUP BY ApplicationId
                                ) AS latest ON udt.ApplicationId = latest.ApplicationId AND udt.CreationTs = latest.max_ts
                                LEFT JOIN tenantinetcsc.EboDetail ed ON ed.id = udt.EboDetailId
                                LEFT JOIN tenantinetcsc.`User` u ON ed.UserId = u.id
                                LEFT JOIN tenantinetcsc.UpeDistrictService uds ON uds.id = udt.UpeDistrictServiceId
                                LEFT JOIN tenantinetcsc.SubDistrict sd ON sd.id = ed.SubDistrictId
                                WHERE udt.UpeDistrictServiceId IN (1)
                        """
        ),
        "service_func": service_function,
    },
    "CHITRAKOOT_SCA": {
        "main_query": text(
            """ SELECT u.apna_id AS EBO_ID,u.f_name AS USERNAME,uu.vle_id AS VLE_ID,tur.service_code AS SERVICE_CODE,tur.application_no AS VENDOR_REFERENCE,
                            tur.req_app_count AS APPLICATION_COUNT,tur.created_at AS SERVICE_DATE,tur.rate AS RATE,tur.total_amt AS TOTAL_AMOUNT FROM iti_portal.tb_up_request tur
                            JOIN (
                            SELECT MAX(id) AS min_id
                            FROM iti_portal.tb_up_request
                            WHERE DATE(created_at) BETWEEN :start_date AND :end_date
                            AND service_code NOT LIKE 'IS'
                            GROUP BY application_no
                            ) uniq ON tur.id = uniq.min_id
                            LEFT JOIN iti_portal.users u ON u.id = tur.users_id
                            LEFT JOIN iti_portal.users_up uu ON u.id = uu.users_id
                            order by SERVICE_DATE ASC 
                        """
        ),
        "Date_diff_query": text(
            """ SELECT u.apna_id AS EBO_ID,u.f_name AS USERNAME,uu.vle_id AS VLE_ID,tur.service_code AS SERVICE_CODE,tur.application_no AS VENDOR_REFERENCE,
                                tur.created_at AS SERVICE_DATE,tur.rate AS RATE,tur.total_amt AS TOTAL_AMOUNT
                                FROM iti_portal.tb_up_request tur
                                JOIN (
                                SELECT 
                                    MAX(id) AS max_id
                                FROM iti_portal.tb_up_request
                                WHERE service_code NOT LIKE 'IS' 
                                AND application_no IN :app_ids
                                GROUP BY application_no
                                ) AS latest ON tur.id = latest.max_id
                                LEFT JOIN iti_portal.users u ON u.id = tur.users_id
                                LEFT JOIN iti_portal.users_up uu ON u.id = uu.users_id
                                WHERE tur.service_code NOT LIKE 'IS'
                                AND tur.application_no IN :app_ids
                            """
        ),
        "service_func": service_function,
    },
    "CHITRAKOOT_IS": {
        "main_query": text(
            """ SELECT u.apna_id AS EBO_ID,u.f_name AS USERNAME,uu.vle_id AS VLE_ID,tur.service_code AS SERVICE_CODE,tur.quota_id AS VENDOR_REFERENCE,
                            tur.req_app_count AS APPLICATION_COUNT,tur.created_at AS SERVICE_DATE,tur.rate AS RATE,tur.total_amt AS TOTAL_AMOUNT
                            FROM iti_portal.tb_up_request tur
                            JOIN (
                            SELECT 
                                MAX(id) AS max_id
                            FROM iti_portal.tb_up_request
                            WHERE service_code LIKE 'IS'
                            AND DATE(Created_at) BETWEEN :start_date AND :end_date
                            GROUP BY quota_id
                            ) AS latest ON tur.id = latest.max_id
                            LEFT JOIN iti_portal.users u ON u.id = tur.users_id
                            LEFT JOIN iti_portal.users_up uu ON u.id = uu.users_id
                            WHERE tur.service_code LIKE 'IS'
                            AND DATE(tur.Created_at) BETWEEN :start_date AND :end_date;
                        """
        ),
        "Date_diff_query": text(
            """ SELECT u.apna_id AS EBO_ID,u.f_name AS USERNAME,uu.vle_id AS VLE_ID,tur.service_code AS SERVICE_CODE,tur.quota_id AS VENDOR_REFERENCE,
                                tur.req_app_count AS APPLICATION_COUNT,tur.created_at AS SERVICE_DATE,tur.rate AS RATE,tur.total_amt AS TOTAL_AMOUNT
                                FROM iti_portal.tb_up_request tur
                                JOIN (
                                SELECT 
                                    MAX(id) AS max_id
                                FROM iti_portal.tb_up_request
                                WHERE service_code LIKE 'IS'
                                AND quota_id IN :app_ids
                                GROUP BY quota_id
                                ) AS latest ON tur.id = latest.max_id
                                LEFT JOIN iti_portal.users u ON u.id = tur.users_id
                                LEFT JOIN iti_portal.users_up uu ON u.id = uu.users_id
                                WHERE tur.service_code LIKE 'IS'
                                AND tur.quota_id IN :app_ids
                            """
        ),
        "service_func": service_function,
    },
    "MANUAL_TB": {
        "main_query": text(
            """ SELECT u.UserName IHUB_USERNAME,u.FirstName as NAME,mtb.Name as BANK_NAME,mtt.VerifiedUtrNo as VENDOR_REFERENCE,mtt.AccountNo as ACC_NO,
                mtt.UtrNo as UTR_NO,mtt.Amount as AMOUNT,mtt.ManualTbStatusType as service_status,mtt.creationTs as SERVICE_DATE
                FROM ihubcore.ManualTbTransaction mtt 
                LEFT JOIN ihubcore.ManualTbBank mtb on mtb.id = mtt.manualTbbankId
                LEFT JOIN tenantinetcsc.EboDetail ed on ed.id = mtt.EboDetailId
                LEFT JOIN tenantinetcsc.`User` u on u.id = ed.UserId 
                where DATE(mtt.CreationTs) BETWEEN :start_date AND :end_date
                """
        ),
        "Date_diff_query": text(
            """ SELECT u.UserName IHUB_USERNAME,u.FirstName as NAME,mtb.Name as BANK_NAME,mtt.VerifiedUtrNo as VENDOR_REFERENCE,mtt.AccountNo as ACC_NO,
                mtt.UtrNo as UTR_NO,mtt.Amount as AMOUNT,mtt.ManualTbStatusType as service_status,mtt.creationTs as SERVICE_DATE
                FROM ihubcore.ManualTbTransaction mtt
                left join ihubcore.ManualTbBank mtb on mtb.id = mtt.manualTbbankId
                LEFT JOIN tenantinetcsc.EboDetail ed on ed.id = mtt.EboDetailId
                LEFT JOIN tenantinetcsc.`User` u on u.id = ed.UserId
                WHERE mtt.VerifiedUtrNo IN :app_ids
                """
        ),
        "service_func": service_function,
    },
}
