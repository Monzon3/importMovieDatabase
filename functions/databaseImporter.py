''' After the new database and its tables have been created, export values from the original Access database
into a file named 'Peliculas.xlsx' and store it in /resources. It is very important to re-index the whole Excel
table again before, to avoid having missing values in the Id column.

Then execute this script to obtain all unique values 
from the original fields 'Disco', 'Calidad', 'Idioma' and 'Pais' and import their values into the new database.

Finally, the Genres table is also populated with the values from the .json file 'ListGenres'.'''

from configparser import ConfigParser
import common.dbConnector as dbConnector
import json
import pandas as pd
import pymysql as sql

def get_unique_values(col, dataFrame):
    ''' This function will obtain the unique values from a specific column in the original database
    to populate the new 'Countries', 'Languages', 'Qualities and 'Storage' tables in the new database

    - col: Name of the column in the original Access database to work with
    - dataFrame: Full original Access database, in a Pandas DataFrame structure'''

    # Obtain unique values from 'col' column to insert into the new 'col' table
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


def update_database(conn, db, series, table, col):
    ''' This function will populate the tables 'Countries', 'Languages', 'Qualities' and 'Storage' 
    in the new database with the unique values obtained from the original database
    
    - conn: MySQL connector
    - db: MySQL cursor
    - series: Series of values to insert into the new database's tables
    - table: Name of the table in which to insert these values mentioned above 
    - col: Name of the column, within the table, in which to insert values.'''

    # Full languages' names need to be added so the NOT NULL constraint is met
    if table == 'Languages':
        # Load the configuration.ini file
        config = ConfigParser()
        config.read('./config/configuration.ini')
        with open(config.get('Aux_files', 'languages_list'), encoding = 'utf-8') as f:
            lang_list = json.load(f)

    for val in series:
        try:
            if table == 'Languages':
                sql_query = f'''INSERT INTO MovieDB.{table} ({col}, LangComplete)
                            VALUES (\'{val}\', \'{lang_list[val]}\')'''
            else:
                sql_query = f'INSERT INTO MovieDB.{table} ({col}) VALUES (\'{val}\')'
            
            db.execute(sql_query)
            conn.commit()
        
        except sql.Error as error:
            print(f'Error while updating the table {table} using:\n {sql_query}: ', error)


def update_genres(conn, db, list_genres):
    ''' This function will populate the tables 'Genres' and 'Genres_Categories' 
    from the database using the .json file 'ListGenres', where all options should be included.
    
    - conn: MySQL connector
    - db: MySQL cursor
    - list_genres: Path from Configuration.ini to the ListGenres.json'''

    with open(list_genres, encoding = 'utf-8') as f:
        genres = json.load(f)

    list = []
    for i in genres.keys():
        if genres[i]['Category'] not in list: 
            list.append(genres[i]['Category'])
            sql_query = f'''INSERT INTO MovieDB.Genre_Categories (Category) VALUES 
                            (\'{genres[i]['Category']}\')'''
            try:
                db.execute(sql_query)
                conn.commit()

            except sql.Error as error:
                print(f'Error while updating the table \'Genre_Categories\' using: \n {sql_query}: ', error)

        try:
            sql_query = f'''SELECT id FROM MovieDB.Genre_Categories 
                            WHERE Category = \'{genres[i]['Category']}\'''' 

            db.execute(sql_query)
            res = db.fetchone()

            sql_query = f'''INSERT INTO MovieDB.Genres (CategoryID, Name) VALUES 
                            ({res[0]}, \'{genres[i]['Name']}\')''' 

            db.execute(sql_query)
            conn.commit() 

        except sql.Error as error:
            print(f'Error while updating the table \'Genres\' using:\n {sql_query}: ', error)


def import_database():
    print('- Importing database structure and secondary tables...')
    # Load the configuration.ini file
    config = ConfigParser()
    config.read('./config/configuration.ini')

    # Connect to MySQL 'MovieDB'
    [conn, db] = dbConnector.connect_to_db()

    # Import data from Excel file (obtained from the original Access database)
    excel_path = config.get('Aux_files', 'excel_database')
    movie_database = pd.read_excel(excel_path)

    # Get unique values for the following columns to populate the new tables in the database
    disc = get_unique_values('Disco', movie_database)
    quality = get_unique_values('Calidad', movie_database)
    lang = get_unique_values('IdiomaAudio', movie_database)
    country = get_unique_values('Pais', movie_database)

    # Import the unique values into the new database
    update_database(conn, db, disc, 'Storage', 'Device')
    update_database(conn, db, quality, 'Qualities', 'Quality')
    update_database(conn, db, lang, 'Languages', 'LangShort')
    update_database(conn, db, country, 'Countries', 'Country')
    update_genres(conn, db, config.get('Aux_files', 'genres_list'))

    str1 = 'If no errors have been shown, all values have been correctly imported'
    str2 = "into the tables 'Qualities', 'Storage', 'Languages', 'Countries' and 'Genres'"
    print(' '.join([str1, str2]))

    db.close()
    conn.close()
    print("\nDisconnected from database 'MovieDB'\n")