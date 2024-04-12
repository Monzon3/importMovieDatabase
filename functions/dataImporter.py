''' After the new database structure has been created and the reference tables populated, 
the real data from the Access database is imported into the new "Main" table. 

To do this, all reference tables should be looked up in order to exchange the values from
the original database for the new corresponding IDs.

After this is done, the tables Audio_in_movie, Genre_in_movie and Subs_in_movie
will be populated using the information from the original database and the reference tables.'''

from configparser import ConfigParser
import common.dbConnector as dbConnector
import pandas as pd
import pymysql as sql

def import_df_to_db(conn, db, dataFrame, mod=''):
    ''' After finding the new ID values for the fields 'Quality', 'Storage' and 'Country'
    the whole database from the Excel file is imported into the new SQL database. These tables will have
    ID values instead of strings in the 'Main' table.
    
    - conn: MySQL connector
    - db: MySQL cursor
    - dataFrame: dataFrame to import into database
    - mod: Empty to operate with MovieDB and '_test' to operate with MovieDB_test.'''

    for i in range(dataFrame.shape[0]):
        sql_query = f"""INSERT INTO MovieDB{mod}.Main (Title, OriginalTitle, StorageID, QualityID, Year,
                    CountryID, Length, Screenplay, Score, Image) 
                    VALUES 
                    ('{dataFrame.loc[i, 'Titulo']}', '{dataFrame.loc[i, 'TituloOriginal']}', 
                      {dataFrame.loc[i, 'Disco']}, {dataFrame.loc[i, 'Calidad']}, 
                      {dataFrame.loc[i, 'Año']}, {dataFrame.loc[i, 'Pais']}, 
                      {dataFrame.loc[i, 'Duracion']}, '{dataFrame.loc[i, 'Guion']}', 
                      {dataFrame.loc[i, 'Puntuacion']}, '{dataFrame.loc[i, 'Imagen']}');"""
        # It is important to keep the " and ' in the sql_query as they are, even if it is the 
        # opposite from the rest of the code, because there are some entries that also have
        # " inside their text, so that would create errors when inserting into the database

        try:
            db.execute(sql_query)
            conn.commit()
        
        except sql.Error as error:
            print(f"Error during the execution of {sql_query}", error)


def import_directors(conn, db, film_id, value, mod=''):
    ''' This function will read all values from 'Directors' column in the Excel database, 
    split them using the ', ' character for the entries with two or more directors, 
    and then populate the table Director_in_movie with the names found.
    
    - conn: MySQL connector
    - db: MySQL cursor
    - filmID: Film ID to obtain its directors
    - value: Entry to process and divide, if necessary
    - mod: Empty to operate with MovieDB and '_test' to operate with MovieDB_test.'''

    id = []
    # Obtain corresponding DirectorID from the new 'Directors' table for each director in 'value'
    if value != '':                 # Not empty
        if value.find(',') != -1:   # More than one director
            directors = value.split(', ')
            for d in directors:
                sql_query = f"SELECT id FROM MovieDB{mod}.Directors WHERE Name = '{d}';"
                db.execute(sql_query)
                id.append(db.fetchone()[0])

        elif value.find(',') == -1: # One director
            sql_query = f"SELECT id FROM MovieDB{mod}.Directors WHERE Name = '{value}';"
            db.execute(sql_query)
            id.append(db.fetchone()[0])

    # Populate Director_in_movie with the obtained values
    for i in id:
        try:
            sql_query = f"""INSERT INTO MovieDB{mod}.Director_in_movie (filmID, directorID) VALUES 
                          ({film_id}, {i});"""
            db.execute(sql_query)
            conn.commit()
        
        except sql.Error as error:
            print(f"Error during the execution of {sql_query}", error)


def import_genres(conn, db, film_id, value, mod=''):
    ''' This function will read all values from 'Genero' column
    in the Excel database, split them using the ',' character, for the movies
    with more than one genre and then populate the table 'Genre_in_movie' with the genres found.
    
    - conn: MySQL connector
    - db: MySQL cursor
    - filmID: Film ID to obtain genres
    - value: Entry to process and divide, if necessary
    - mod: Empty to operate with MovieDB and '_test' to operate with MovieDB_test.'''
    
    # Obtain corresponding GenreID from the new 'Genres' table for each genre in 'value'
    if value != '-':
        genres = value.split(',')[0:-1]    # value always ends with ',' so the last element is not needed

        for i in genres:
            # All genres have a category defined between brackets that is not needed for now
            pos1 = i.find('[')
            pos2 = i.find(']')
            category = i[(pos1 + 1):pos2]
            genre = i[(pos2 + 2):]

            # Some genres have changed in the new database, so to be able to find them:
            if genre == 'Palma de Oro': genre = 'Cannes - Palma de Oro'
            if genre == 'Mejor película' and category == 'Premios - Goya': 
                genre = 'Goya - Mejor película'
            if genre == 'Mejor director': genre = 'Oscars - Mejor director'
            if genre == 'Mejor extranjera': genre = 'Oscars - Mejor extranjera'
            if genre == 'Mejor fotografía': genre = 'Oscars - Mejor fotografía'
            if genre == 'Mejor película' and category == 'Premios - Óscars':
                genre = 'Oscars - Mejor película'

            sql_query = f"SELECT id FROM MovieDB{mod}.Genres WHERE Name = '{genre}';"
            db.execute(sql_query)
            res = db.fetchone()
            
            if res != None:
                try:
                    sql_query = f"""INSERT INTO MovieDB{mod}.Genre_in_movie (filmID, genreID) 
                                    VALUES ({film_id}, {res[0]});"""
                    db.execute(sql_query)
                    conn.commit()
                
                except sql.Error as error:
                    print(f"Error during the execution of {sql_query}", error)


def import_languages(conn, db, film_id, category, value, mod=''):
    ''' This function will read all values from 'IdiomaAudio' and 'IdiomSubtitulos' columns
    in the Excel database, split them using the '-' character for the movies
    with two languages in 'Audio' or 'Subs' columns, and then populate the tables 
    'Audio_in_movie' and 'Subs_in_movie' with the languages found.
    
    - conn: MySQL connector
    - db: MySQL cursor
    - filmID: Film ID to obtain audio and subtitles' languages
    - category: Either 'Audio' or 'Subs', to define which table in the database to populate
    - value: Entry to process and divide, if necessary
    - mod: Empty to operate with MovieDB and '_test' to operate with MovieDB_test.'''

    id = []
    # To match the new values in the database the following two have to be changed,
    # because in the original database they were 'May' and 'Var', instead of 'Maya' and 'Varios'
    # (see function 'get_unique_values' in 01_databaseImporter.py for reference)
    if value == 'Maya': 
        value = 'May'
    
    if value == 'Var':
        value = 'Varios'

    # Obtain corresponding LanguageID from the new 'Languages' table for each language in 'value'
    if value != '-':                    # Not empty
        if value.find('-') != -1:       # Two languages in the entry
            [lang1, lang2] = value.split('-')
            sql_query = f"SELECT id FROM MovieDB{mod}.Languages WHERE LangShort = '{lang1}';"
            db.execute(sql_query)
            id.append(db.fetchone()[0])

            sql_query = f"SELECT id FROM MovieDB{mod}.Languages WHERE LangShort = '{lang2}';"
            db.execute(sql_query)
            id.append(db.fetchone()[0])

        elif value.find('-') == -1:     # Single language in the entry
            sql_query = f"SELECT id FROM MovieDB{mod}.Languages WHERE LangShort = '{value}';"
            db.execute(sql_query)
            id.append(db.fetchone()[0])

    # Populate Audio_in_movie and Subs_in_movie with the obtained values
    for i in id:
        try:
            sql_query = f"""INSERT INTO MovieDB{mod}.{category}_in_movie (filmID, languageID) VALUES
                            ({film_id}, {i});"""
            db.execute(sql_query)
            conn.commit()
        
        except sql.Error as error:
            print(f"Error during the execution of {sql_query}", error)


def obtainID(db, table, value):
    ''' Function to obtain the ID of a given string 'value' in a given 'table'.

    - db: MySQL cursor
    - table: Name of the table in which to look into for the 'value'
    - value: String value to look for in the database and obtain its ID.'''

    sql_query = f"SELECT id FROM MovieDB.{table} WHERE Name = '{value}';"
    db.execute(sql_query)

    return db.fetchone()[0]


def import_data():
    print("4. Importing registers into 'Main' table...")
    # Load the configuration.ini file
    config = ConfigParser()
    config.read('./config/configuration.ini')

    # Import data from Excel file (obtained from the original Access database)
    excel_path = config.get('Aux_files', 'excel_database')
    movie_database = pd.read_excel(excel_path)

    # Connect to MySQL 'MovieDB'
    [conn, db] = dbConnector.connect_to_db()
    [conn_test, db_test] = dbConnector.connect_to_db('_test')

    # Obtain IDs from database for former 'Pais', 'Disco' and 'Calidad' string values
    # and overwrite dataframe values
    for i in range(movie_database.shape[0]):
        movie_database.loc[i, 'Pais'] = obtainID(db, 
                                                table='Countries', 
                                                value=movie_database.loc[i, 'Pais'])
        movie_database.loc[i, 'Disco'] = obtainID(db,
                                                table='Storage', 
                                                value=movie_database.loc[i, 'Disco'])
        movie_database.loc[i, 'Calidad'] = obtainID(db,
                                                table='Qualities', 
                                                value=movie_database.loc[i, 'Calidad'])

    # Import dataFrame with updated 'Countries', 'Storage' and 'Qualities' values into SQL
    movie_database['Titulo'] = movie_database['Titulo'].str.replace("'","''")
    movie_database['TituloOriginal'] = movie_database['TituloOriginal'].str.replace("'","''")
    movie_database['Director'] = movie_database['Director'].str.replace("'","''")
    movie_database['Guion'] = movie_database['Guion'].str.replace("'","''")
    import_df_to_db(conn, db, movie_database)
    import_df_to_db(conn_test, db_test, movie_database, '_test')

    # After the old database has been imported into the new one, the following can be done without breaking
    # any constraints related with FOREIGN KEYS.
    for i in range(movie_database.shape[0]):
        # Obtain Audios and Subs from 'movie_database' for each film 
        # and generate 'Audio_in_movie' and 'Subs_in_movie' tables
        import_languages(conn, db, movie_database.loc[i, 'Id'], 'Audio', movie_database.loc[i, 'IdiomaAudio'])
        import_languages(conn_test, db_test, movie_database.loc[i, 'Id'], 'Audio', 
                        movie_database.loc[i, 'IdiomaAudio'], '_test')
        import_languages(conn, db, movie_database.loc[i, 'Id'], 'Subs', movie_database.loc[i, 'IdiomaSubtitulos'])
        import_languages(conn_test, db_test, movie_database.loc[i, 'Id'], 'Subs', 
                        movie_database.loc[i, 'IdiomaSubtitulos'], '_test')

        # Do the same with genres
        import_genres(conn, db, movie_database.loc[i, 'Id'], movie_database.loc[i, 'Genero'])
        import_genres(conn_test, db_test, movie_database.loc[i, 'Id'], movie_database.loc[i, 'Genero'], '_test')

        # And with Directors
        import_directors(conn, db, movie_database.loc[i, 'Id'], movie_database.loc[i, 'Director'])
        import_directors(conn_test, db_test, movie_database.loc[i, 'Id'], 
                        movie_database.loc[i, 'Director'], '_test')

    print("- All data imported into the new 'MovieDB' and 'MovieDB_test' databases")

    db.close()
    conn.close()
    print("\n-- Disconnected from database 'MovieDB' --\n")

    db_test.close()
    conn_test.close()
    print("\n-- Disconnected from database 'MovieDB_test' --\n")