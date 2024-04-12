''' After the new database and its tables have been created, the import_database function will obtain
all unique values from the original fields 'Disco', 'Calidad, 'IdiomaAudio', 'País' and 'Director' to
populate the newly normalized tables. 

Finally, the new Genres tables are also populated with the values from the .json file 'ListGenres'.

In order to import everything properly, the exported entries from the original Access database
should be into a file named 'Peliculas.xlsx' and stored in /resources.
It is very important to re-index the whole Excel
table again before running the script, to avoid having missing values in the Id column.'''

from configparser import ConfigParser
import common.dbConnector as dbConnector
import json
import pandas as pd
import pymysql as sql

def get_unique_values(col, dataFrame):
    ''' This function will obtain the unique values from a specific column in the original database
    to populate the new 'Countries', 'Languages', 'Qualities', 'Storage' and 'Directors'
    tables in the new database.

    - col: Name of the column in the original Access database to work with
    - dataFrame: Full original Access database, in a Pandas DataFrame structure'''

    # Obtain unique values from 'col' column to insert into the new 'col' table
    series = dataFrame[-dataFrame.duplicated(col)][col]

    # Some of the 'Director' registers are multiple entries in the original database
    if col == 'Director':
        aux_series = pd.Series()
        for i in range(series.size):
            if series.iloc[i].find(',') != -1:  # Isolate directors from multiple-entries and append to aux series
                aux_series = pd.concat([aux_series, pd.Series(series.iloc[i].split(', '))])  
                 
        series = pd.concat([series, aux_series])        # Append aux to original series
        series = series[-series.str.find(',')>0]        # Delete multiple-entries, which are now duplicated
        
        # Delete again duplicated values (this is needed as now there will be duplicated entries again)
        series = series[-series.duplicated()]  

    # 'IdiomaAudio' (Language) values are processed differently 
    if col == 'IdiomaAudio': 
        # This values will be updated for the future version of the database
        series = series.str.replace('Maya', 'May')
        series = series.str.replace('Var', 'Varios')

        # Latin is added (there is one movie with subtitles in Latin)
        series = pd.concat([series, pd.Series(['Lat'])])

        # Delete double-values and finish off
        series = series[-series.str.find('-')>0] 

    return series.sort_values()


def update_database(conn, db, series, table, mod:str=""):
    ''' This function will populate the tables 'Countries', 'Languages', 'Qualities',
    'Storage' and 'Directors' in the new database with the unique values 
    obtained from the original database
    
    - conn: MySQL connector
    - db: MySQL cursor
    - series: Series of values to insert into the new database's tables
    - table: Name of the table in which to insert these values mentioned above 
    - mod: Empty to operate with MovieDB and '_test' to operate with MovieDB_test.'''

    # Full languages' names need to be added so the NOT NULL constraint is met
    if table == 'Languages':
        # Load the configuration.ini file
        config = ConfigParser()
        config.read('./config/configuration.ini')
        with open(config.get('Aux_files', 'languages_list'), encoding = 'utf-8') as f:
            lang_list = json.load(f)

    for val in series:
        val = val.replace("'", "''")        # For directors with ' in their name
        try:
            if table == 'Languages':
                sql_query = f"""INSERT INTO MovieDB{mod}.{table} (LangShort, LangComplete)
                            VALUES ('{val}', '{lang_list[val]}');"""
            else:
                sql_query = f"INSERT INTO MovieDB{mod}.{table} (Name) VALUES ('{val}');"
            
            db.execute(sql_query)
            conn.commit()
        
        except sql.Error as error:
            print(f"Error while updating the table {table} using:\n {sql_query}: ", error)


def update_genres(conn, db, series, mod:str=""):
    ''' This function will populate the tables 'Genres' and 'Genres_Categories' 
    from the database using the .json file 'ListGenres', where all options should be included.
    
    - conn: MySQL connector
    - db: MySQL cursor
    - series: Series of values to insert into the new database's tables
    - mod: Empty to operate with MovieDB and '_test' to operate with MovieDB_test.'''

    list = []
    for i in series.keys():
        if series[i]['Category'] not in list: 
            list.append(series[i]['Category'])
            sql_query = f"""INSERT INTO MovieDB{mod}.Genre_Categories (Name) VALUES 
                            ('{series[i]['Category']}');"""
            try:
                db.execute(sql_query)
                conn.commit()

            except sql.Error as error:
                print(f"Error while updating the table 'Genre_Categories' using: \n {sql_query}: ", error)

        try:
            sql_query = f"""INSERT INTO MovieDB{mod}.Genres (CategoryID, Name) VALUES 
                            ((SELECT id FROM MovieDB{mod}.Genre_Categories 
                            WHERE Name = '{series[i]['Category']}'), 
                            '{series[i]['Name']}');""" 

            db.execute(sql_query)
            conn.commit() 

        except sql.Error as error:
            print(f"Error while updating the table 'Genres' using:\n {sql_query}: ", error)


def import_database():
    print("3. Populating auxiliary tables with their unique values...")
    # Load the configuration.ini file
    config = ConfigParser()
    config.read('./config/configuration.ini')

    # Connect to MySQL 'MovieDB' and 'MovieDB_test'
    [conn, db] = dbConnector.connect_to_db()
    [conn_test, db_test] = dbConnector.connect_to_db('_test')

    # Import data from Excel file (obtained from the original Access database)
    excel_path = config.get('Aux_files', 'excel_database')
    movie_database = pd.read_excel(excel_path)

    # Get the unique values for the following columns to populate the axuiliary tables in the database
    # and store them into lists
    disc = get_unique_values('Disco', movie_database)
    quality = get_unique_values('Calidad', movie_database)
    lang = get_unique_values('IdiomaAudio', movie_database)
    country = get_unique_values('Pais', movie_database)
    director = get_unique_values('Director', movie_database)

    # Import the lists into the auxiliary tables of both the new database and the test database
    update_database(conn, db, disc, 'Storage')
    update_database(conn_test, db_test, disc, 'Storage', '_test')

    update_database(conn, db, quality, 'Qualities')
    update_database(conn_test, db_test, quality, 'Qualities', '_test')

    update_database(conn, db, lang, 'Languages')
    update_database(conn_test, db_test, lang, 'Languages', '_test')

    update_database(conn, db, country, 'Countries')
    update_database(conn_test, db_test, country, 'Countries', '_test')

    update_database(conn, db, director, 'Directors')
    update_database(conn_test, db_test, director, 'Directors', '_test')

    list_genres = config.get('Aux_files', 'genres_list')
    with open(list_genres, encoding = 'utf-8') as f:
        genres = json.load(f)
    update_genres(conn, db, genres)
    update_genres(conn_test, db_test, genres, '_test')

    str1 = "If no errors have been shown, all values have been correctly imported"
    str2 = "into the tables 'Qualities', 'Storage', 'Languages', 'Countries', 'Directors', 'Genres' and 'Genre_Categories'"
    str3 = "within MovieDB and MovieDB_test databases"
    print(" ".join([str1, str2, str3]))

    db.close()
    db_test.close()
    
    conn.close()
    print("\n-- Disconnected from database 'MovieDB' --\n")

    conn_test.close()
    print("-- Disconnected from database 'MovieDB_test' --\n")