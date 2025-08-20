import pandas as pd
from db_connector import get_db_connection
from logger_config import logger
from sqlalchemy.exc import SQLAlchemyError, OperationalError, DatabaseError
from sqlalchemy import text
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

engine = get_db_connection()

# Retry config for DB operations
DB_RETRY_CONFIG = {
    "stop": stop_after_attempt(3),
    "wait": wait_exponential(multiplier=1, min=1, max=10),
    "retry": retry_if_exception_type((OperationalError, DatabaseError)),
    "reraise": True,
}


@retry(**DB_RETRY_CONFIG)
def execute_sql_with_retry(query, params=None):
    logger.info("Executing SQL with retry")
    try:
        with engine.connect() as connection:
            result = connection.execute(query, params or {})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
    except Exception as e:
        logger.error(f"SQL execution error: {e}")
        raise


def inet_count():
    logger.info("Fetching INET Users Count")

    queries = {
        "Active_list": """
            SELECT u.UserName, u.FirstName, u.MobileNo, u.Email, DATE(pph.ExpireTs) as Expiry_Date
            FROM tenantinetcsc.UserPurchasedPackage pph
            LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
            WHERE DATE(pph.ExpireTs) > CURRENT_DATE() AND u.UserRoleId = 2 and pph.PackageId in (1,4)
        """,
        "TN_Active_list": """
            SELECT u.UserName, u.FirstName, u.MobileNo, u.Email, DATE(pph.ExpireTs) as Expiry_Date
            FROM tenantinetcsc.`User` u 
            LEFT JOIN tenantinetcsc.UserPurchasedPackage pph ON pph.UserId = u.id
            WHERE DATE(pph.ExpireTs) > CURRENT_DATE() AND u.UserRoleId = 2 AND u.UserName LIKE 'TN%'
        """,
        "UP_Active_list": """
            SELECT u.UserName,u.VleId, u.FirstName, u.MobileNo, u.Email, DATE(pph.ExpireTs) as Expiry_Date
            FROM tenantinetcsc.`User` u 
            LEFT JOIN tenantinetcsc.UserPurchasedPackage pph ON pph.UserId = u.id
            WHERE DATE(pph.ExpireTs) > CURRENT_DATE() AND u.UserRoleId = 2 AND u.UserName LIKE 'UP%'
        """,
        "current_month_expiry_list": """
            SELECT u.UserName, u.FirstName,u.VleId, u.MobileNo, u.Email, DATE(pph.ExpireTs) as Expiry_Date
            FROM tenantinetcsc.UserPurchasedPackage pph
            LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
            WHERE MONTH(pph.ExpireTs) = MONTH(CURDATE())
            AND YEAR(pph.ExpireTs) = YEAR(CURDATE())
            AND u.UserRoleId = 2
        """,
        "AP_Active_list": """
             SELECT u.UserName, u.FirstName, u.MobileNo, u.Email, DATE(pph.ExpireTs) as Expiry_Date
            FROM tenantinetcsc.`User` u 
            LEFT JOIN tenantinetcsc.UserPurchasedPackage pph ON pph.UserId = u.id
            WHERE DATE(pph.ExpireTs) > CURRENT_DATE() AND u.UserRoleId = 2 AND u.UserName LIKE 'ap%'
        """,
        "last_month_inactive_list": """
            SELECT u.UserName, u.FirstName,u.VleId, u.MobileNo, u.Email, DATE(pph.ExpireTs) as Expiry_Date
            FROM tenantinetcsc.UserPurchasedPackage pph
            LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
            WHERE pph.ExpireTs >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m-01')
            AND pph.ExpireTs < DATE_FORMAT(CURDATE(), '%Y-%m-01')
            AND u.UserRoleId = 2
        """,
    }

    result = {}

    try:
        for key, query_str in queries.items():
            logger.info(f"Running query for: {key}")
            df = execute_sql_with_retry(text(query_str))
            if df.empty:
                logger.warning(f"No data found for: {key}")
            result[key] = df
        if all(df.empty for df in result.values()):
            logger.info("No data found for any query")
            return "No data found"
        else:
            logger.info("INET Users Count fetched successfully")
            return result
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def ebodetailed_data(from_date=None, to_date=None, tenant_name=None, ebo_status=None):
    logger.info("Fetching EBO Detailed Data")
    result = {}
    try:
        tenant_type = SERVICE_CONFIGS[tenant_name]
        # Debugging line to check tenant type
        status = ebo_status
        if status not in tenant_type["ebo_status"]:
            logger.warning(f"Invalid EBO status: {status}")
            return "Invalid EBO status"

        if status == "active":
            query = tenant_type["activequery"]
        elif status == "emi-not-paid":
            query = tenant_type["inactivequery_emi"]
        else:
            query = tenant_type["inactivequery"]
        logger.info(f"Using query for tenant: {tenant_name} with status: {status}")
        params = {"from_date": from_date, "to_date": to_date}
        df = execute_sql_with_retry(text(query), params=params)
        df["Expiry_Date"] = pd.to_datetime(
            df["Expiry_Date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        if df.empty:
            logger.warning(f"No data found for EBO Detailed Data for {tenant_type}")
            return "No data found"
        else:
            logger.info("EBO Detailed Data fetched successfully")
            result[tenant_name] = df
            return result
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


SERVICE_CONFIGS = {
    "I-NET TN Users": {
        "ebo_status": ["active", "inactive"],
        "activequery": """SELECT u.UserName, u.FirstName as Customer_Name, u.MobileNo as Phone_Num, u.Email,p.Name as Package_Name, DATE(pph.ExpireTs) as Expiry_Date
                FROM tenantinetcsc.UserPurchasedPackage pph
                LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
                left join tenantinetcsc.Package p on p.id = pph.PackageId
                WHERE DATE(pph.ExpireTs) >= :from_date AND u.UserRoleId = 2  AND u.UserName LIKE 'TN%' and pph.PackageId in (1,4)""",
        "inactivequery": """SELECT u.UserName , u.FirstName  as Customer_Name, u.MobileNo as Phone_Num, u.Email,p.Name as Package_Name, DATE(pph.ExpireTs) as Expiry_Date
                FROM tenantinetcsc.UserPurchasedPackage pph
                LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
                left join tenantinetcsc.Package p on p.id = pph.PackageId
                WHERE  DATE(pph.ExpireTs) BETWEEN :from_date AND :to_date
                AND u.UserRoleId = 2 AND u.UserName LIKE 'TN%' and pph.PackageId in (1,4)""",
        "service_function": ebodetailed_data,
    },
    "I-NET UP Users": {
        "ebo_status": ["active", "inactive"],
        "activequery": """SELECT u.UserName, u.FirstName as Customer_Name, u.MobileNo as Phone_Num, u.Email,p.Name as Package_Name, DATE(pph.ExpireTs) as Expiry_Date
                FROM tenantinetcsc.UserPurchasedPackage pph
                LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
                left join tenantinetcsc.Package p on p.id = pph.PackageId
                WHERE DATE(pph.ExpireTs) >= :from_date AND u.UserRoleId = 2  AND u.UserName LIKE 'up01%' and pph.PackageId in (1,4)""",
        "inactivequery": """SELECT u.UserName , u.FirstName  as Customer_Name, u.MobileNo as Phone_Num, u.Email,p.Name as Package_Name, DATE(pph.ExpireTs) as Expiry_Date
                FROM tenantinetcsc.UserPurchasedPackage pph
                LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
                left join tenantinetcsc.Package p on p.id = pph.PackageId
                WHERE  DATE(pph.ExpireTs) BETWEEN :from_date AND :to_date
                AND u.UserRoleId = 2 AND u.UserName LIKE 'up01%' and pph.PackageId in (1,4)""",
        "service_function": ebodetailed_data,
    },
    "I-NET PACCS Users": {
        "ebo_status": ["active", "inactive"],
        "activequery": """SELECT u.UserName, u.FirstName as Customer_Name, u.MobileNo as Phone_Num, u.Email,p.Name as Package_Name, DATE(pph.ExpireTs) as Expiry_Date
                FROM tenantinetcsc.UserPurchasedPackage pph
                LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
                left join tenantinetcsc.Package p on p.id = pph.PackageId
                WHERE DATE(pph.ExpireTs) >= :from_date AND u.UserRoleId = 1""",
        "inactivequery": """SELECT u.UserName , u.FirstName  as Customer_Name, u.MobileNo as Phone_Num, u.Email,p.Name as Package_Name, DATE(pph.ExpireTs) as Expiry_Date
                FROM tenantinetcsc.UserPurchasedPackage pph
                LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
                left join tenantinetcsc.Package p on p.id = pph.PackageId
                WHERE  DATE(pph.ExpireTs) BETWEEN :from_date AND :to_date
                AND u.UserRoleId = 1""",
        "service_function": ebodetailed_data,
    },
    "UPe-District Sultanpur PS Users": {
        "ebo_status": ["active", "inactive"],
        "activequery": """    
                select * from (SELECT u.UserName, u.FirstName as Customer_Name, u.MobileNo as Phone_Num, u.Email,p.Name as Package_Name, DATE(pph.ExpireTs) as Expiry_Date
                FROM tenantinetcsc.UserPurchasedPackage pph
                LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
                left join tenantinetcsc.Package p on p.id = pph.PackageId
                WHERE u.UserRoleId = 2 AND u.UserName LIKE 'UPPS%')a where  YEAR(Expiry_Date) = '1971' OR DATE(Expiry_Date) >= :from_date""",
        "inactivequery": """Select * from (SELECT u.UserName , u.FirstName  as Customer_Name, u.MobileNo as Phone_Num, u.Email,p.Name as Package_Name, DATE(pph.ExpireTs) as Expiry_Date
                FROM tenantinetcsc.UserPurchasedPackage pph
                LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
                left join tenantinetcsc.Package p on p.id = pph.PackageId
                WHERE u.UserRoleId = 2 AND u.UserName LIKE 'UPPS%' and YEAR(pph.expireTS) != '1971'
                ) a where DATE(a.Expiry_Date) BETWEEN :from_date AND :to_date""",
        "service_function": ebodetailed_data,
    },
    "ITI UP Users": {
        "ebo_status": ["active", "inactive", "emi-not-paid"],
        "activequery": """ 
                select u.apna_id as UserName,uu.vle_id as Vle_Id,u.f_name as Customer_Name,u.phone_no as Phone_Num,u.email_id as Email,DATE(u.next_due_date) as Expiry_Date 
                from iti_portal.users u
                left join iti_portal.users_up uu on uu.users_id =u.id
                where u.mas_user_type_id = 4 and u.up_vle_status =0 and u.next_due_date > CURRENT_DATE()
                UNION ALL
                Select u.apna_id as UserName,uu.vle_id as Vle_Id,u.f_name as Customer_Name,u.phone_no as Phone_Num,u.email_id as Email,DATE(u.next_due_date) as Expiry_Date 
                FROM iti_portal.users u
                    left join iti_portal.users_up uu on uu.users_id = u.id
                where u.apna_id like '%UP%' and u.mas_user_type_id='4' and u.up_vle_status in(2) 
                and DATE(u.next_due_date) > CURRENT_DATE()""",
        "inactivequery_emi": """select apna_id as UserName,vle_id as Vle_Id,f_name as Customer_Name,phone_no as Phone_Num,email_id as Email,DATE(next_due_date) as Expiry_Date,
                    total_due_dates,payment_done,payment_not_done from(
                SELECT
                u.id,u.apna_id ,uu.vle_id,u.phone_no,u.f_name,u.email_id,u.next_due_date,   
                COUNT(*) AS total_due_dates,
                SUM(CASE WHEN uei.payment_sts = 1 THEN 1 ELSE 0 END) AS payment_done,
                SUM(CASE WHEN uei.payment_sts = 0 THEN 1 ELSE 0 END) AS payment_not_done
                FROM
                iti_portal.up_emi_installments uei 
                left join iti_portal.users u on u.id = uei.users_id 
                left join iti_portal.users_up uu on uu.users_id =u.id
                where u.apna_id like '%UP%' and u.mas_user_type_id='4' and u.up_vle_status in(2)
                GROUP BY
                uei.users_id
                ) a where a.payment_not_done != 0""",
        "inactivequery": """select apna_id as UserName,vle_id as Vle_Id,f_name as Customer_Name,phone_no as Phone_Num,email_id as Email,DATE(next_due_date) as Expiry_Date,
                total_due_dates,payment_done,payment_not_done from(
                SELECT
                u.id,u.apna_id ,uu.vle_id,u.phone_no,u.f_name,u.email_id,u.next_due_date,   
                COUNT(*) AS total_due_dates,
                SUM(CASE WHEN uei.payment_sts = 1 THEN 1 ELSE 0 END) AS payment_done,
                SUM(CASE WHEN uei.payment_sts = 0 THEN 1 ELSE 0 END) AS payment_not_done
                FROM
                iti_portal.up_emi_installments uei
                left join iti_portal.users u on u.id = uei.users_id 
                left join iti_portal.users_up uu on uu.users_id =u.id
                where u.apna_id like '%UP%' and u.mas_user_type_id='4' and u.up_vle_status in(2)
                GROUP BY
                uei.users_id
                ) a where a.payment_not_done = 0 and DATE(a.next_due_date) Between :from_date and :to_date""",
        "service_function": ebodetailed_data,
    },
    "UPe-District Chitrakoot PS Users": {
        "ebo_status": ["active", "inactive"],
        "activequery": """ 
                Select * from (   
                Select u.apna_id as UserName,uu.vle_id as Vle_Id,u.f_name as Customer_Name,u.phone_no as Phone_Num,u.email_id as Email,DATE(u.next_due_date) as Expiry_Date  from iti_portal.users u 
                left join iti_portal.users_up uu on u.id =uu.users_id
                where u.apna_id  like 'UPPS%'
                    ) a where YEAR(Expiry_date) ='1971' or DATE(Expiry_date) BETWEEN :from_date AND :to_date   
            """,
        "inactivequery": """Select * from (   
                Select u.apna_id as UserName,uu.vle_id as Vle_Id,u.f_name as Customer_Name,u.phone_no as Phone_Num,u.email_id as Email,DATE(u.next_due_date) as Expiry_Date  from iti_portal.users u 
                left join iti_portal.users_up uu on u.id =uu.users_id
                where u.apna_id  like 'UPPS%'
                    ) a where YEAR(Expiry_date) !='1971' AND DATE(Expiry_date) < :from_date""",
        "service_function": ebodetailed_data,
    },
}
