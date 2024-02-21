from dotenv import load_dotenv
import os
import pymysql

load_dotenv()

def connect_to_db():
    ''' This function returns the connector and cursor objects to work with the database 

    - db_name: The name of the database path in the Configuration.ini file.'''

    MySQL_hostname = '127.0.0.1'
    sql_username = os.getenv("SQL_ADMIN_USERNAME")
    sql_password = os.getenv("SQL_ADMIN_PASSWORD")
    sql_database = os.getenv("MYSQL_DB")

    # Connect with the database
    connector = pymysql.connect(host=MySQL_hostname,
                        port=3306, 
                        user=sql_username,
                        passwd=sql_password, 
                        db=sql_database)

    cursor = connector.cursor()
    print(f'Connected to database \'{sql_database}\' \n')                                       

    return connector, cursor