from configparser import ConfigParser
import sqlite3 as sql


def enable_fk(db_cursor):
    '''
    This function enables the usage of foreign keys in the database 
    and then checks whether they have been successfully enabled.
    '''
    sql_command = 'PRAGMA foreign_keys = ON'
    db_cursor.execute(sql_command)

    sql_command = 'PRAGMA foreign_keys'
    fkInfo = db_cursor.execute(sql_command)
    for i in fkInfo:
        if i[0] == 1:
            print('Foreign keys have been successfully enabled\n')