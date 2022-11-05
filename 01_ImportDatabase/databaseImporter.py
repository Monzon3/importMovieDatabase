''' After the new database and its tables have been created, export values from the original Access database
into a file named 'Peliculas.xlsx' and store it in \Resources. 

Then, copy the newly created 'Peliculas.db' file into /01_ImportDatabase 
and execute this script to obtain all unique values from the original fields 'Disco', 'Calidad', 
'Idioma' and 'Pais' and import them values into the new database.

Finally, the genres table is also populated with the values from the .json file 'ListGenres'.'''

from configparser import ConfigParser
import dbConnector
import json
import pandas as pd
import sqlite3 as sql

def get_unique_values(col, dataFrame):
    ''' This function will obtain the unique values from a specific column in the original database
    to populate the new 'Calidad', 'Disco', 'Idioma' and 'Pais' tables in the new database

    - col: Name of the column in the original Access database to work with
    - dataFrame: Full original Access database, in a Pandas DataFrame structure'''

    # Obtain different values from 'col' column to insert into the new 'col' table
    series = dataFrame[-dataFrame.duplicated(col)][col]

   # 'IdiomaAudio' (Language) values are processed differently 
    if col == 'IdiomaAudio': 
        # This values will be updated for the future version of the database
        series = series.apply(lambda x: x.replace('Maya', 'May'))
        series = series.apply(lambda x: x.replace('Var', 'Varios'))

        # Latin is added (there is one movie with subtitles in Latin)
        series = pd.concat([series, pd.Series(['Lat'])])

        # Delete double-values and finish off
        series = series[-series.str.find('-')>0]

    series = series.sort_values()

    return series


def update_database(series, table, col):
    ''' This function will populate the tables 'Calidad', 'Disco', 'Idioma' and 'Pais' in the new database
    with the unique values obtained from the original database
    
    - series: Series of values to insert into the new dataframe's tables
    - table: Name of the table in which to insert these above mentioned values 
    - col: Name of the column, within the table, in which to insert values
    '''

    # Full languages' names need to be added so the NOT NULL constraint is met
    if table == 'Idioma':
        with open(config.get('Aux_files', 'languages_list'), encoding = 'utf-8') as f:
            lang_list = json.load(f)

    for val in series:
        try:
            if table == 'Idioma':
                sql_query = 'INSERT INTO ' + table + ' (' + col + ''', IdiomaCompleto) 
                            VALUES (\'''' + val + '\', \'' + lang_list[val] + '\')'
                sql_query = f'''INSERT INTO {table} ({col}, IdiomaCompleto)
                            VALUES (\'{val}\', \'{lang_list[val]}\')'''
            else:
                sql_query = f'INSERT INTO {table} ({col}) VALUES (\'{val}\')'
            
            db.execute(sql_query)
            conn.commit()
        
        except sql.Error as error:
            print(f'Error while updating the table {table} using:\n {sql_query}: ', error)


def update_genres():
    ''' This function will populate the table 'Generos' included in the database using the 
    .json file 'ListGenres', where all options should be included.
    '''

    with open(config.get('Aux_files', 'genres_list'), encoding = 'utf-8') as f:
        genres = json.load(f)

    for i in genres.keys():
        try:
            sql_query = f'''INSERT INTO Genero (Categoria, Nombre) VALUES 
                        (\'{genres[i]['Category']}\', \'{genres[i]['Name']}\')'''            
            db.execute(sql_query)
            conn.commit() 

        except sql.Error as error:
            print(f'Error while updating the table \'Genres\' using:\n {sql_query}: ', error)


if __name__ == '__main__':
    # Load the configuration.ini file
    config = ConfigParser()
    config.read('C:\\MisCosas\\Documentos\\MovieDatabase\\configuration.ini')

    # Connect with 'import_database' which is found in the [Paths] section of .ini file
    [conn, db] = dbConnector.connect_to_db('import_database')

    # Import data from Excel file (obtained from the original Access database)
    excel_path = config.get('Aux_files', 'excel_database')
    movie_database = pd.read_excel(excel_path)

    # Get unique values for the following columns to populate the new tables in the database
    disc = get_unique_values('Disco', movie_database)
    quality = get_unique_values('Calidad', movie_database)
    lang = get_unique_values('IdiomaAudio', movie_database)
    country = get_unique_values('Pais', movie_database)

    # Import the unique values into the new database
    update_database(disc, 'Disco', 'Disco')
    update_database(quality, 'Calidad', 'Calidad')
    update_database(lang, 'Idioma', 'IdiomaAbreviado')
    update_database(country, 'Pais', 'Pais')
    update_genres()

    print('\n')
    print('If no errors have been shown, all values have been correctly imported')
    print('  into the tables \'Calidad\', \'Disco\', \'Idioma\' and \'Pais\' in the new database')

    db.close()
    conn.close()