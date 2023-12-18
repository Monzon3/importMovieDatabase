''' After the main table and the reference tables have been populated, extract the data 
from the database, get the text values for the corresponding IDs of genres, languages, quality and so on
and check with the original excel database that all the information has been copied exactly. 

To do this, all reference tables should be looked up in order to exchange the new ID values from
the database into their original text values.'''

from configparser import ConfigParser
import common.dbConnector as dbConnector
import pandas as pd

def obtain_genres(film_id):
    ''' This function will get the ID of the movie from the new database, 
    search into Genre_in_file for that ID and return all values found into one string.
    If there were more than one genre, they shall be separated by a comma, to match the original ones.'''

    sql_query = f"SELECT genreID from Genre_in_file WHERE filmID = '{film_id}'"
    db.execute(sql_query)
    record = db.fetchall()

    genres = []
    if len(record) != 0:
        for i in record:
            sql_query = f"SELECT Name FROM Genres WHERE id = '{i[0]}'"
            sql_query = f'''SELECT Genre_Categories.Category, Genres.Name 
                        FROM Genres 
                        INNER JOIN Genre_Categories 
                        ON Genres.CategoryID=Genre_Categories.id
                        WHERE Genres.id={i[0]}'''
            genres_raw = db.execute(sql_query).fetchall()
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


def obtain_languages(film_id):
    ''' This function will get the ID of the movie from the new database, search into
    audio_in_file and subs_in_file tables for that ID and return all values found into one string. 
    If there were more than one language, they shall be separated with a hyphen.'''

    # Get all audio and subs IDs for the given movie
    # Audios:
    audios = "-"
    subs = "-"
    for category in ["Audio", "Subs"]:
        sql_query = f"SELECT languageID from {category}_in_file WHERE filmID = '{film_id}'"
        db.execute(sql_query)
        record = db.fetchall()

        lang = []
        if len(record) != 0:
            for i in record:
                sql_query = f"SELECT LangShort FROM Languages WHERE id = '{i[0]}'"
                res = db.execute(sql_query).fetchone()[0]
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


def obtain_val(table, field, id):
    ''' Function to obtain the value for a given 'id' in a given 'table'.

    - table: Name of the table in which to look into
    - field: Name of the column, within that 'table', in which to look into
    - id: ID to look for in the database and obtain its original text value.'''

    sql_query = f"SELECT {field} FROM {table} WHERE id = '{id}'"
    val = db.execute(sql_query).fetchone()

    return val[0]


if __name__ == '__main__':
    # Load the configuration.ini file
    config = ConfigParser()
    config.read('./config/configuration.ini')

    # Import data from Excel file (obtained from the original Access database)
    excel_path = config.get('Aux_files', 'excel_database')
    original_database = pd.read_excel(excel_path)

    # # Connect with 'test_database' which is in [Paths] section from .ini file
    [conn, db] = dbConnector.connect_to_db('test_database')

    # Obtain values from database for new 'Pais', 'Disco' and 'Calidad' ID values
    # and update dataframe values
    new_database = pd.read_sql_query('SELECT * FROM Main', conn) 

    for i in range(new_database.shape[0]):
        new_database.loc[i, 'CountryID'] = obtain_val(table='Countries', 
                                                     field='Country', 
                                                     id=new_database.loc[i, 'CountryID'])
        new_database.loc[i, 'StorageID'] = obtain_val(table='Storage', 
                                                     field='Device', 
                                                     id=new_database.loc[i, 'StorageID'])
        new_database.loc[i, 'QualityID'] = obtain_val(table='Qualities', 
                                                     field='Quality', 
                                                     id=new_database.loc[i, 'QualityID'])

    # A different function is used to get the Languages and Genres for each film
    # First of all add 'IdiomaAudio', 'IdiomaSubtitulos' and 'Genre' columns 
    new_database.insert(5, 'IdiomaAudio', '')
    new_database.insert(6, 'IdiomaSubtitulos', '')
    new_database.insert(12, 'Genre', '') 

    for i in range(new_database.shape[0]):
        # Obtain Audios and Subs IDs from 'Audio_in_file' and 'Subs_in_file' tables 
        # for each film in new_database and get their original text values from 'Languages' table
        audios, subs = obtain_languages(new_database.loc[i, 'id'])
        new_database.loc[i, 'IdiomaAudio'] = audios
        new_database.loc[i, 'IdiomaSubtitulos'] = subs

        # Do the same with genres
        genres = obtain_genres(new_database.loc[i, 'id'])
        new_database.loc[i, 'Genre'] = genres

    #Change names of new database to match the old ones
    new_database.columns = ['Id', 'Titulo', 'TituloOriginal', 'Disco', 'Calidad', 'IdiomaAudio',
    'IdiomaSubtitulos', 'Año', 'Pais', 'Duracion', 'Director', 'Guion', 'Genero', 'Puntuacion', 'Imagen']

    df = pd.merge(original_database, new_database, how='left', indicator=True)
    diff = df[df['_merge']!='both']
    
    if diff.shape[0]>0:
        print('Se han encontrado diferencias en las películas siguientes (por ID):')
        print(diff.loc[:, 'Id'])
    else:
        print('¡Las dos bases de datos son idénticas!')

    db.close()
    conn.close()