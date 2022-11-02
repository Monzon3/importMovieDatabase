'''
After the new database and its tables have been created, export values from the original Access database
into a file named 'Peliculas.xlsx' and store it in \Resources. 
Then copy the newly created 'Peliculas.db' file into this path (\01_ImportDatabase) 
and execute this script to obtain all unique values from the original fields 'Disco', 'Calidad', 
'Idioma' and 'Pais'. 
Then import those values into the new database.
'''

from configparser import ConfigParser
import dbConnector
import json
import pandas as pd
import sqlite3 as sql

def get_individual_values(col, dataFrame):
    '''
        - col: Name of the column in the original Access database to work with
        - dataFrame: Full original Access database, in a Pandas DataFrame structure
    '''
    # Obtain different values from 'col' column to insert into the new 'col' table
    series = dataFrame[-dataFrame.duplicated(col)][col]

   # 'IdiomaAudio' (Language) values are processed differently 
    if col == 'IdiomaAudio': 
        # This values will be updated for the future versions
        series[series=='Maya'] = 'May' 
        series[series=='Var'] = 'Varios' 

        # Latin is added (there is one movie with subtitles in Latin)
        s_aux = pd.Series(['Lat'])
        series = pd.concat([series, s_aux])

        # Delete double-values and finish off
        series = series[-series.str.find('-')>0]

    series = series.sort_values()

    return series


def update_database(series, table, col):
    '''
        - series: Series of values to insert into the new dataframe's tables
        - table: Name of the table in which to insert these above mentioned values 
        - col: Name of the column, within the table, in which to insert values
    '''

    # Full languages names need to be added so the NOT NULL constraint is met
    if table == 'Idioma':
        with open(config.get('Aux_files', 'languages_list'), encoding = 'utf-8') as f:
            lang_list = json.load(f)

    for val in series:
        try:
            if table == 'Idioma':
                sql_query = 'INSERT INTO ' + table + ' (' + col + ''', IdiomaCompleto) 
                            VALUES (\'''' + val + '\', \'' + lang_list[val] + '\')'
            else:
                sql_query = 'INSERT INTO ' + table + ' (' + col + ') VALUES (\'' + val + '\')'
            
            db.execute(sql_query)
            conn.commit()
        
        except sql.Error as error:
            print(f'Error while updating the table {table}: ', error)


def update_genres():
    '''
    This function will populate the table 'Generos' included in the database using the 
    .json file 'ListGenres', where all options should be included.
    '''

    with open(config.get('Aux_files', 'genres_list'), encoding = 'utf-8') as f:
        genres = json.load(f)

    for i in genres.keys():
        try:
            sql_query = 'INSERT INTO Genero (Categoria, Nombre) VALUES (\'' + genres[i]['Category'] + '\', \'' + genres[i]['Name'] + '\')'
            
            db.execute(sql_query)
            conn.commit() 

        except sql.Error as error:
            print(f'Error while updating the table \'Genres\': ', error)


if __name__ == '__main__':
    # Load the configuration.ini file
    config = ConfigParser()
    config.read('C:\\MisCosas\\Documentos\\MovieDatabase\\configuration.ini')

   # Connect with the database
    db_path = config.get('Paths', 'import_database')
    conn = sql.connect(db_path)
    db = conn.cursor()

    # Enable foreign keys
    dbConnector.enable_fk(db)

    # Import data from Excel file (obtained from the original Access database)
    excel_path = config.get('Aux_files', 'excel_database')
    df = pd.read_excel(excel_path)

    # Get unique values for the following columns to populate the new tables in the database
    disc = get_individual_values('Disco', df)
    quality = get_individual_values('Calidad', df)
    lang = get_individual_values('IdiomaAudio', df)
    country = get_individual_values('Pais', df)

    # Import the values into the new database
    update_database(disc, 'Disco', 'Disco')
    update_database(quality, 'Calidad', 'Calidad')
    update_database(lang, 'Idioma', 'IdiomaAbreviado')
    update_database(country, 'Pais', 'Pais')
    update_genres()

    db.close()
    conn.close()