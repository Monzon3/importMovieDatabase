'''
After the new database structure has been created and the reference tables populated, 
the real data from the Access database is imported into the new "Main" table. 
To do this, all reference tables should be looked up in order to exchange the values from
the database for the new corresponding IDs.
'''

from configparser import ConfigParser
import pandas as pd
import sqlite3 as sql

def obtainID(field, value):
    sql_query = f'SELECT id FROM {field} WHERE {field} = \'{value}\''
    equivalent = db.execute(sql_query).fetchone()

    return equivalent[0]


if __name__ == '__main__':
    # Load the configuration.ini file
    config = ConfigParser()
    config.read('C:\\MisCosas\\Documentos\\MovieDatabase\\configuration.ini')

    # Import data from Excel file (obtained from the original Access database)
    excel_path = config.get('Aux_files', 'excel_database')
    df = pd.read_excel(excel_path)

    # Connect with the database
    db_path = config.get('Paths', 'import_database')
    conn = sql.connect(db_path)
    db = conn.cursor()

    # Obtain IDs from database for old 'Pais', 'Disco' and 'Calidad' values
    # and update dataframe values
    for i in range(df.shape[0]):
        df.loc[i, 'Pais'] = obtainID('Pais', df.loc[i, 'Pais'])
        df.loc[i, 'Disco'] = obtainID('Disco', df.loc[i, 'Disco'])
        df.loc[i, 'Calidad'] = obtainID('Calidad', df.loc[i, 'Calidad'])

    # Export values to Excel to be able to compare with the original ones to verify the process
    # See macro inside DataChecker.xlsx
    excel_newPath = config.get('Aux_files', 'excel_newDatabase')
    df.to_excel(excel_newPath, index = False)

    db.close()
    conn.close()