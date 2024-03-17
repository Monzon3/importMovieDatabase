from dotenv import load_dotenv
import os
import pymysql

load_dotenv()

def connect_to_db(mod:str=''):
    ''' This function returns the connector and cursor objects to work with the database.
    
    It is important to grant privileges to the admin user in the MySQL database before importing the database.'''

    MySQL_hostname = 'db'   # The name of the mysql Docker container
    sql_username = os.getenv("MYSQL_USER")
    sql_password = os.getenv("MYSQL_PASSWORD")
    
    if mod == '':
        sql_database = os.getenv("MYSQL_DATABASE")
    elif mod == '_test':
        sql_database = os.getenv("MYSQL_TEST_DATABASE")

    # Connect with the database
    connector = pymysql.connect(host=MySQL_hostname,
                        port=3306, 
                        user=sql_username,
                        passwd=sql_password, 
                        db=sql_database)

    cursor = connector.cursor()
    print(f"-- Connected to database '{sql_database}' --\n")                                       

    return connector, cursor