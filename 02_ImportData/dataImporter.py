'''
After the new database structure has been created and the reference tables populated, 
the real data from the Access database is imported into the new "Main" table. 
To do this, all reference tables should be looked up in order to exchange the values from
the database for the new corresponding IDs.
Before running this script, copy the database from /01_ImportDatabase/Peliculas.db into
/test.db
'''

from configparser import ConfigParser
import dbConnector
import pandas as pd
import sqlite3 as sql


def import_df_to_db(dataFrame, connector, cursor):
    for i in range(dataFrame.shape[0]):
        tit = dataFrame.loc[i, 'Titulo'].replace("\'","\'\'")
        origTit = dataFrame.loc[i, 'TituloOriginal'].replace("\'","\'\'")
        disc = dataFrame.loc[i, 'Disco']
        qt = dataFrame.loc[i, 'Calidad']
        year = dataFrame.loc[i, 'Año']
        country = dataFrame.loc[i, 'Pais']
        dur = dataFrame.loc[i, 'Duracion']
        dir = dataFrame.loc[i, 'Director'].replace("\'","\'\'")
        script = dataFrame.loc[i, 'Guion'].replace("\'","\'\'")
        sql_query = f"""INSERT INTO Main (Titulo, TituloOriginal, DiscoID, CalidadID, Year,
                    PaisID, Duracion, Director, Guion) 
                    VALUES 
                    (\'{tit}\', \'{origTit}\', {disc}, {qt}, {year}, 
                    {country}, {dur}, \'{dir}\', \'{script}\')"""

        try:
            cursor.execute(sql_query)
            connector.commit()
        
        except sql.error as error:
            print(f'Error during the execution of {sql_query}', error)
            
    print('All data imported into the new database')


def obtainID(field, value):
    '''
    - field: Name of the table in which to look into (which is the same as the column's name within that table)
    - value: Value to look for in the database and obtain its ID
    '''
    sql_query = f'SELECT id FROM {field} WHERE {field} = \'{value}\''
    equivalent = db.execute(sql_query).fetchone()

    return equivalent[0]


if __name__ == '__main__':
    # Load the configuration.ini file
    config = ConfigParser()
    config.read('C:\\MisCosas\\Documentos\\MovieDatabase\\configuration.ini')

    # Import data from Excel file (obtained from the original Access database)
    excel_path = config.get('Aux_files', 'excel_database')
    movie_database = pd.read_excel(excel_path)

    # Connect with the database located in [Paths] 'test_database' from .ini file
    [conn, db] = dbConnector.connect_to_db('test_database')

    # Obtain IDs from database for old 'Pais', 'Disco' and 'Calidad' values
    # and update dataframe values
    for i in range(movie_database.shape[0]):
        movie_database.loc[i, 'Pais'] = obtainID('Pais', movie_database.loc[i, 'Pais'])
        movie_database.loc[i, 'Disco'] = obtainID('Disco', movie_database.loc[i, 'Disco'])
        movie_database.loc[i, 'Calidad'] = obtainID('Calidad', movie_database.loc[i, 'Calidad'])

    # Import dataFrame with updated 'Pais', 'Disco' and 'Calidad' values into SQL
    import_df_to_db(movie_database, conn, db)

    # Export values to Excel to be able to compare with the original ones to verify the process
    # See macro inside DataChecker.xlsm to verify that the imported data is the same as the original
    excel_newPath = config.get('Aux_files', 'excel_newDatabase')
    movie_database.to_excel(excel_newPath, index = False)

    db.close()
    conn.close()