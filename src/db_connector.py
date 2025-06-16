from sqlalchemy import create_engine
from config import CONFIG

def get_db_connection():
    db = CONFIG["db"]
    return create_engine(f"mariadb+pymysql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}")
get_db_connection()