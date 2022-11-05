from configparser import ConfigParser
import sqlite3 as sql


def connect_to_db(db_name):
    ''' This function returns the connector and cursor objects to work with the database 

    - db_name: The name of the database path within the Configuration.ini file.'''

    # Read path from .ini Config file
    config = ConfigParser()
    config.read('C:\\MisCosas\\Documentos\\MovieDatabase\\configuration.ini')
    db_path = config.get('Paths', db_name)

    # Connect with the database
    connector = sql.connect(db_path)
    cursor = connector.cursor()
    print(f'Connected to database \'{db_path}\'')

    # Always enable foreign keys
    enable_fk(cursor)

    return connector, cursor


def enable_fk(db_cursor):
    ''' This function enables the usage of foreign keys in the database 
    and then checks whether they have been successfully enabled.'''
    sql_command = 'PRAGMA foreign_keys = ON'
    db_cursor.execute(sql_command)

    sql_command = 'PRAGMA foreign_keys'
    fkInfo = db_cursor.execute(sql_command)
    for i in fkInfo:
        if i[0] == 1:
            print('Foreign keys have been successfully enabled\n')