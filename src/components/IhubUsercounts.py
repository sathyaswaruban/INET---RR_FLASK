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
            WHERE DATE(pph.ExpireTs) > CURRENT_DATE() AND u.UserRoleId = 2
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
            SELECT u.UserName, u.FirstName, u.MobileNo, u.Email, DATE(pph.ExpireTs) as Expiry_Date
            FROM tenantinetcsc.UserPurchasedPackage pph
            LEFT JOIN tenantinetcsc.`User` u ON u.id = pph.UserId
            WHERE pph.ExpireTs >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m-01')
            AND pph.ExpireTs < DATE_FORMAT(CURDATE(), '%Y-%m-01')
            AND u.UserRoleId = 2
        """
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

    
