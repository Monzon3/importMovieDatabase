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

def import_df_to_db(conn, db, dataFrame):
    ''' After finding the new ID values for the fields 'Quality', 'Storage' and 'Country'
    the whole database from the Excel file is imported into the new SQL database.
    
    - conn: MySQL connector
    - db: MySQL cursor'''

    for i in range(dataFrame.shape[0]):
        # ' are replaced by '' so SQL is able to process them properly
        tit = dataFrame.loc[i, 'Titulo']
        origTit = dataFrame.loc[i, 'TituloOriginal']
        disc = dataFrame.loc[i, 'Disco']
        qt = dataFrame.loc[i, 'Calidad']
        year = dataFrame.loc[i, 'Año']
        country = dataFrame.loc[i, 'Pais']
        dur = dataFrame.loc[i, 'Duracion']
        score = dataFrame.loc[i, 'Puntuacion']
        script = dataFrame.loc[i, 'Guion']
        img = dataFrame.loc[i, 'Imagen']
        sql_query = f"""INSERT INTO MovieDB.Main (Title, OriginalTitle, StorageID, QualityID, Year,
                    CountryID, Length, Screenplay, Score, Image) 
                    VALUES 
                    ('{tit}', '{origTit}', {disc}, {qt}, {year}, 
                    {country}, {dur}, '{script}', {score}, '{img}');"""

        try:
            db.execute(sql_query)
            conn.commit()
        
        except sql.Error as error:
            print(f'Error during the execution of {sql_query}', error)


def import_directors(conn, db, film_id, value):
    ''' This function will read all values from 'Directors' column in the Excel database, 
    split them using the ', ' character for the entries with two or more directors, 
    and then populate the table Director_in_movie with the names found.
    
    - conn: MySQL connector
    - db: MySQL cursor
    - filmID: Film ID to obtain genres
    - value: Entry to process and divide, if necessary'''

    id = []
    # Obtain corresponding DirectorID from the new 'Directors' table for each director in 'value'
    if value != '':                 # Not empty
        if value.find(',') != -1:   # More than one director
            directors = value.split(', ')
            for d in directors:
                sql_query = f"SELECT id FROM MovieDB.Directors WHERE Name = '{d}';"
                db.execute(sql_query)
                id.append(db.fetchone()[0])

        elif value.find(',') == -1: # One director
            sql_query = f"SELECT id FROM MovieDB.Directors WHERE Name = '{value}';"
            db.execute(sql_query)
            id.append(db.fetchone()[0])

    # Populate Director_in_movie with the obtained values
    for i in id:
        sql_query = f"INSERT INTO MovieDB.Director_in_movie (filmID, directorID) VALUES ({film_id}, {i});"

        try:
            db.execute(sql_query)
            conn.commit()
        
        except sql.Error as error:
            print(f'Error during the execution of {sql_query}', error)


def import_genres(conn, db, film_id, value):
    ''' This function will read all values from 'Genero' column
    in the Excel database, split them using the ',' character for the movies
    with more than one genre and then populate the table 'Genre_in_movie' with the genres found.
    
    - conn: MySQL connector
    - db: MySQL cursor
    - filmID: Film ID to obtain genres
    - value: Entry to process and divide, if necessary'''
    
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

            sql_query = f"SELECT id FROM MovieDB.Genres WHERE Name = '{genre}';"
            db.execute(sql_query)
            res = db.fetchone()
            
            if res != None:
                sql_query = f'''INSERT INTO MovieDB.Genre_in_movie (filmID, genreID) 
                            VALUES ({film_id}, {res[0]});'''
                try:
                    db.execute(sql_query)
                    conn.commit()
                
                except sql.Error as error:
                    print(f'Error during the execution of {sql_query}', error)


def import_languages(conn, db, film_id, category, value):
    ''' This function will read all values from 'IdiomaAudio' and 'IdiomSubtitulos' columns
    in the Excel database, split them using the '-' character for the movies
    with two languages in 'Audio' or 'Subs' columns and then populate the tables 
    'Audio_in_movie' and 'Subs_in_movie' with the languages found.
    
    - conn: MySQL connector
    - db: MySQL cursor
    - filmID: Film ID to obtain audio and subtitles' languages
    - category: Either 'Audio' or 'Subs', to define which table in the database to populate
    - value: Entry to process and divide, if necessary'''

    id = []
    # To match the new values in the database the following two have to be changed,
    # because in the original database they were 'May' and 'Var', instead of 'Maya' and 'Varios'
    # (see function 'get_unique_values' in 01_databaseImporter.py for reference)
    if value == 'Maya': 
        value = 'May'
    
    if value == 'Var':
        value = 'Varios'

    # Obtain corresponding LanguageID from the new 'Languages' table for each language in 'value'
    if value != '-':        # Not empty
        if value.find('-') != -1:
            [lang1, lang2] = value.split('-')
            sql_query = f"SELECT id FROM MovieDB.Languages WHERE LangShort = '{lang1}';"
            db.execute(sql_query)
            id.append(db.fetchone()[0])

            sql_query = f"SELECT id FROM MovieDB.Languages WHERE LangShort = '{lang2}';"
            db.execute(sql_query)
            id.append(db.fetchone()[0])

        elif value.find('-') == -1:
            sql_query = f"SELECT id FROM MovieDB.Languages WHERE LangShort = '{value}';"
            db.execute(sql_query)
            id.append(db.fetchone()[0])

    # Populate Audio_in_movie and Subs_in_movie with the obtained values
    for i in id:
        if category == 'Audio':
            sql_query = f'INSERT INTO MovieDB.Audio_in_movie (filmID, languageID) VALUES ({film_id}, {i});'

        elif category == 'Subs':
            sql_query = f'INSERT INTO MovieDB.Subs_in_movie (filmID, languageID) VALUES ({film_id}, {i});'

        try:
            db.execute(sql_query)
            conn.commit()
        
        except sql.Error as error:
            print(f'Error during the execution of {sql_query}', error)


def obtainID(db, table, field, value):
    ''' Function to obtain the ID of a given 'value' in a given 'table'.

    - db: MySQL cursor
    - table: Name of the table in which to look into
    - field: Name of the column, within that 'table', in which to look into
    - value: Value to look for in the database and obtain its ID.'''

    sql_query = f"SELECT id FROM MovieDB.{table} WHERE {field} = '{value}';"
    db.execute(sql_query)
    id = db.fetchone()

    return id[0]


def import_data():
    print("- 3. Importing registers into 'Main' table...")
    # Load the configuration.ini file
    config = ConfigParser()
    config.read('./config/configuration.ini')

    # Import data from Excel file (obtained from the original Access database)
    excel_path = config.get('Aux_files', 'excel_database')
    movie_database = pd.read_excel(excel_path)

    # Connect to MySQL 'MovieDB'
    [conn, db] = dbConnector.connect_to_db()

    # Obtain IDs from database for former 'Pais', 'Disco' and 'Calidad' values
    # and update dataframe values
    for i in range(movie_database.shape[0]):
        movie_database.loc[i, 'Pais'] = obtainID(db, 
                                                table='Countries', 
                                                field='Country', 
                                                value=movie_database.loc[i, 'Pais'])
        movie_database.loc[i, 'Disco'] = obtainID(db,
                                                table='Storage', 
                                                field='Device', 
                                                value=movie_database.loc[i, 'Disco'])
        movie_database.loc[i, 'Calidad'] = obtainID(db,
                                                table='Qualities', 
                                                field='Quality', 
                                                value=movie_database.loc[i, 'Calidad'])

    # Import dataFrame with updated 'Countries', 'Storage' and 'Qualities' values into SQL
    movie_database['Titulo'] = movie_database['Titulo'].str.replace("'","''")
    movie_database['TituloOriginal'] = movie_database['TituloOriginal'].str.replace("'","''")
    movie_database['Director'] = movie_database['Director'].str.replace("'","''")
    movie_database['Guion'] = movie_database['Guion'].str.replace("'","''")
    import_df_to_db(conn, db, movie_database)

    # After the old database has been imported into the new one, the following can be done without breaking
    # any constraints related with FOREIGN KEYS.
    for i in range(movie_database.shape[0]):
        # Obtain Audios and Subs from 'movie_database' for each film 
        # and generate 'Audio_in_movie' and 'Subs_in_movie' tables
        import_languages(conn, db, movie_database.loc[i, 'Id'], 'Audio', movie_database.loc[i, 'IdiomaAudio'])
        import_languages(conn, db, movie_database.loc[i, 'Id'], 'Subs', movie_database.loc[i, 'IdiomaSubtitulos'])

        # Do the same with genres
        import_genres(conn, db, movie_database.loc[i, 'Id'], movie_database.loc[i, 'Genero'])

        # And with Directors
        import_directors(conn, db, movie_database.loc[i, 'Id'], movie_database.loc[i, 'Director'])

    print('- All data imported into the new MovieDB database')

    # Export values to Excel to be able to compare with the original ones to verify the process
    # See macro inside DataChecker.xlsm to verify that the imported data is the same as the original
    excel_newPath = config.get('Aux_files', 'excel_newDatabase')
    movie_database.to_excel(excel_newPath, index = False)

    db.close()
    conn.close()
    print("\nDisconnected from database 'MovieDB'\n")