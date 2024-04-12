''' After the main table and the reference tables have been populated, extract the data 
from the database, get the text values for the corresponding IDs of genres, languages, quality and so on
and check with the original excel database that all the information has been copied exactly. 

To do this, all reference tables should be looked up in order to exchange the new ID values from
the database into their original text values.'''

from configparser import ConfigParser
import common.dbConnector as dbConnector
import pandas as pd

def obtain_directors(db, film_id, mod=''):
    ''' This function will get the ID of the movie from the new database, search into
    Director_in_movie table for that ID and return all values found, into one string. 
    If there were more than one director, they shall be separated with a comma.
    
    - db: MySQL cursor
    - filmId: FilmID to look for its director
    - mod: Empty to operate with MovieDB and '_test' to operate with MovieDB_test.'''

    # Get all director IDs for the given movie
    sql_query = f"""SELECT Directors.Name from MovieDB{mod}.Director_in_movie 
                  INNER JOIN MovieDB{mod}.Directors ON Director_in_movie.directorID = Directors.id
                  WHERE filmID = {film_id}
                  ORDER BY Directors.Name;"""
    db.execute(sql_query)
    res = db.fetchall()

    directors = []
    if len(res) != 0:
        for i in res:
            directors.append(i[0])

    if len(directors) == 1:
        directors_str = directors[0]
    else:
        directors_str = ', '.join(directors)

    return directors_str


def obtain_genres(db, film_id, mod=''):
    ''' This function will get the ID of the movie from the new database, 
    search into Genre_in_movie for that ID and return all values found into one string.
    If there were more than one genre, they shall be separated by a comma, to match the original ones.
    
    - db: MySQL cursor
    - filmId: FilmID to look for its genres names
    - mod: Empty to operate with MovieDB and '_test' to operate with MovieDB_test.'''

    # Get all genre IDs for the given movie
    sql_query = f"SELECT genreID from MovieDB{mod}.Genre_in_movie WHERE filmID = {film_id};"
    db.execute(sql_query)
    res = db.fetchall()

    genres = []
    if len(res) != 0:
        for i in res:
            sql_query = f"""SELECT Genre_Categories.Name, Genres.Name 
                        FROM MovieDB{mod}.Genres 
                        INNER JOIN MovieDB{mod}.Genre_Categories 
                        ON Genres.CategoryID = Genre_Categories.id
                        WHERE Genres.id={i[0]};"""
            db.execute(sql_query)
            genres_raw = db.fetchall()
            # Get values out of the Tuple and into str to modify them
            genre1 = genres_raw[0][0]
            genre2 = genres_raw[0][1]

            # Some genres have changed in the new database, so to check them they must go back 
            # their original value
            if genre2 == 'Cannes - Palma de Oro': 
                genre1 = 'Premios - Cannes'
                genre2 = 'Palma de Oro'
            if genre2 == 'Goya - Mejor película': 
                genre1 = 'Premios - Goya'
                genre2 = 'Mejor película'
            if genre2 == 'Oscars - Mejor director':
                genre1 = 'Premios - Óscars'
                genre2 = 'Mejor director' 
            if genre2 == 'Oscars - Mejor extranjera':
                genre1 = 'Premios - Óscars'
                genre2 = 'Mejor extranjera' 
            if genre2 == 'Oscars - Mejor fotografía':
                genre1 = 'Premios - Óscars'
                genre2 = 'Mejor fotografía'
            if genre2 == 'Oscars - Mejor película':
                genre1 = 'Premios - Óscars'
                genre2 = 'Mejor película'

            genres.append('[' + genre1 + '] ' + genre2)

    if len(genres) == 0:
        genres_str = "-"
    elif len(genres) == 1:
        genres_str = f"{genres[0]},"
    else:
        genres_str = ""
        for g in genres:
            genres_str = f"{genres_str}{g}, "

        # Remove the last blank space
        genres_str = genres_str[0:-1]

    return genres_str


def obtain_languages(db, film_id, mod=''):
    ''' This function will get the ID of the movie from the new database, search into
    Audio_in_movie and Subs_in_movie tables for that ID and return all values found into one string. 
    If there were more than one language, they shall be separated with a hyphen.
    
    - db: MySQL cursor
    - filmId: FilmID to look for its audio and subtitles languages
    - mod: Empty to operate with MovieDB and '_test' to operate with MovieDB_test.'''

    # Get all audio and subs IDs for the given movie
    # Audios:
    audios = "-"
    subs = "-"
    for category in ["Audio", "Subs"]:
        sql_query = f"SELECT languageID from MovieDB{mod}.{category}_in_movie WHERE filmID = {film_id}"
        db.execute(sql_query)
        res = db.fetchall()

        lang = []
        if len(res) != 0:
            for i in res:
                sql_query = f"SELECT LangShort FROM MovieDB{mod}.Languages WHERE id = {i[0]};"
                db.execute(sql_query)
                res = db.fetchone()[0]
                # The following is done to match the original values in the database 
                # See function 'import_languages' from 02_dataImporter.py
                if res == 'May': 
                    res = 'Maya'
                
                if res == 'Varios':
                    res = 'Var'

                lang.append(res)

            if len(lang) == 1:
                languages = str(lang[0])
            else:
                languages = str(lang[0]) + "-" + str(lang[1])

            if category == 'Audio':
                audios = languages
            elif category == 'Subs':
                subs = languages 

    return audios, subs


def obtain_val(db, table, id, mod):
    ''' Function to obtain the value for a given 'id' in a given 'table'.

    - db: MySQL cursor
    - table: Name of the table in which to look into
    - id: ID to look for in the database and obtain its original text value
    - mod: Empty to operate with MovieDB and '_test' to operate with MovieDB_test.'''

    sql_query = f"SELECT Name FROM MovieDB{mod}.{table} WHERE id = {id};"
    db.execute(sql_query)

    return db.fetchone()[0]


def check_data(mod=''):
    print("5. Checking the values from MySQL are exactly the same as those in the Original Excel database...")
    # Load the configuration.ini file
    config = ConfigParser()
    config.read('./config/configuration.ini')

    # Import data from Excel file (obtained from the original Access database)
    excel_path = config.get('Aux_files', 'excel_database')
    original_database = pd.read_excel(excel_path)

    # Connect to MySQL 'MovieDB'
    [conn, db] = dbConnector.connect_to_db(mod)

    # Obtain values from database for new 'Pais', 'Disco' and 'Calidad' ID values
    # and update dataframe values
    new_database = pd.read_sql_query(f"SELECT * FROM MovieDB{mod}.Main", conn) 

    for i in range(new_database.shape[0]):
        new_database.loc[i, 'CountryID'] = obtain_val(db, 
                                                    table='Countries', 
                                                    id=new_database.loc[i, 'CountryID'],
                                                    mod=mod)
        new_database.loc[i, 'StorageID'] = obtain_val(db,
                                                    table='Storage', 
                                                    id=new_database.loc[i, 'StorageID'],
                                                    mod=mod)
        new_database.loc[i, 'QualityID'] = obtain_val(db,
                                                    table='Qualities',  
                                                    id=new_database.loc[i, 'QualityID'],
                                                    mod=mod)

    # A different function is used to get the Languages and Genres for each film
    # First of all add 'IdiomaAudio', 'IdiomaSubtitulos', 'Director' and 'Genero' columns 
    new_database.insert(5, 'IdiomaAudio', '')
    new_database.insert(6, 'IdiomaSubtitulos', '')
    new_database.insert(10, 'Director', '')
    new_database.insert(12, 'Genero', '') 

    for i in range(new_database.shape[0]):
        # Obtain Audios and Subs IDs from 'Audio_in_movie' and 'Subs_in_movie' tables 
        # for each film in new_database and get their original text values from 'Languages' table
        audios, subs = obtain_languages(db, new_database.loc[i, 'id'], mod=mod)
        new_database.loc[i, 'IdiomaAudio'] = audios
        new_database.loc[i, 'IdiomaSubtitulos'] = subs

        # Do the same with genres
        genres = obtain_genres(db, new_database.loc[i, 'id'], mod=mod)
        new_database.loc[i, 'Genero'] = genres

        # And with directors
        directors = obtain_directors(db, new_database.loc[i, 'id'], mod=mod)
        new_database.loc[i, 'Director'] = directors

    # Change names of new database to match the old ones
    new_database.columns = ['Id', 'Titulo', 'TituloOriginal', 'Disco', 'Calidad', 'IdiomaAudio',
    'IdiomaSubtitulos', 'Año', 'Pais', 'Duracion', 'Director', 'Guion', 'Genero', 'Puntuacion', 'Imagen']

    df = pd.merge(original_database, new_database, how='left', indicator=True)
    diff = df[df['_merge']!='both']
    
    if diff.shape[0]>0:
        print("Differences have been found in the following movies:")
        for i in range(diff.shape[0]):
            print("Original:")
            print(original_database.iloc[(diff.iloc[i, 0] - 1), : ])
            print("New:")
            print(new_database.iloc[(diff.iloc[i, 0] - 1), : ])
    else:
        print("Both databases are exactly the same!")

    db.close()
    conn.close()

    print(f"\n-- Disconnected from database 'MovieDB{mod}' --\n")