''' The idea is to create the database using this file and leave it as a future reference
to look up which tables are in it, type of data within them, etc.

To create all these tables, the databases MovieDB and MovieDB_test should exist and
since they are accessing the database with the user 'admin', this user should exist
and have the privileges for all actions.'''
import common.dbConnector as dbConnector
import pymysql as sql

def create_tables(mod:str=''):
    ''' 
    - 'mod' variable is a modifier to create the same tables in the _test database. 
    If it's empty, it does not modify the original sql queries and all the tables
    are created in the MovieDB database.    
    '''

    # Connect to MySQL 'MovieDB'
    [conn, db] = dbConnector.connect_to_db(mod)

    # Generate tables
    # Storage (this approach is called database normalization in sql)
    try:
        sql_query = f'''CREATE TABLE MovieDB{mod}.Storage (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Device VARCHAR(20) NOT NULL UNIQUE,
                    PRIMARY KEY(id));'''

        db.execute(sql_query)
        conn.commit()
        print(f'- "Storage" table has been created correctly in MovieDB{mod}')
    
    except sql.Error as error:
        print(f'Error while creating the table "Storage" in MovieDB{mod}', error)

    # Qualities
    try:
        sql_query = f'''CREATE TABLE MovieDB{mod}.Qualities (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Quality VARCHAR(10) NOT NULL UNIQUE,
                    PRIMARY KEY(id));'''

        db.execute(sql_query)
        conn.commit()
        print(f'- "Qualities" table has been created correctly in MovieDB{mod}')
    
    except sql.Error as error:
        print(f'Error while creating the table "Qualities" in MovieDB{mod}', error)

    # Countries
    try:
        sql_query = f'''CREATE TABLE MovieDB{mod}.Countries (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Country VARCHAR(25) NOT NULL UNIQUE,
                    PRIMARY KEY(id));'''

        db.execute(sql_query)
        conn.commit()
        print(f'- "Countries" table has been created correctly in MovieDB{mod}')
    
    except sql.Error as error:
        print(f'Error while creating the table "Countries" in MovieDB{mod}', error)

    # Main table
    try:
        sql_query = f'''CREATE TABLE MovieDB{mod}.Main (
                    id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Title VARCHAR(100) NOT NULL,
                    OriginalTitle VARCHAR(100) NOT NULL,
                    StorageID TINYINT UNSIGNED,
                    QualityID TINYINT UNSIGNED, 
                    Year SMALLINT UNSIGNED NOT NULL CHECK(Year>1880 AND Year<2100),
                    CountryID TINYINT UNSIGNED,
                    Length SMALLINT UNSIGNED NOT NULL,
                    Director VARCHAR(200) NOT NULL,
                    Screenplay VARCHAR(300),
                    Score TINYINT UNSIGNED Check(Score<=10),
                    Image VARCHAR(120),
                    PRIMARY KEY(id),
                    FOREIGN KEY (StorageID) REFERENCES Storage(id)
                    ON DELETE SET NULL ON UPDATE CASCADE,
                    FOREIGN KEY (QualityID) REFERENCES Qualities(id)
                    ON DELETE SET NULL ON UPDATE CASCADE,
                    FOREIGN KEY (CountryID) REFERENCES Countries(id)
                    ON DELETE SET NULL ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print(f'- "Main" table has been created correctly in MovieDB{mod}')

    except sql.Error as error:
        print(f'Error while creating the table "Main" in MovieDB{mod}', error)

    # Languages
    try:
        sql_query = f'''CREATE TABLE MovieDB{mod}.Languages (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    LangShort VARCHAR(6) NOT NULL UNIQUE,
                    LangComplete VARCHAR(15) NOT NULL UNIQUE,
                    PRIMARY KEY(id));'''

        db.execute(sql_query)
        conn.commit()
        print(f'- "Languages" table has been created correctly in MovieDB{mod}')
    
    except sql.Error as error:
        print(f'Error while creating the table "Languages" in MovieDB{mod}', error)

    # Audio_in_movie
    try:
        sql_query = f'''CREATE TABLE MovieDB{mod}.Audio_in_movie (
                    filmID SMALLINT UNSIGNED NOT NULL,
                    languageID TINYINT UNSIGNED NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (languageID) REFERENCES Languages(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print(f'- "Audio_in_movie" table has been created correctly in MovieDB{mod}')
    
    except sql.Error as error:
        print(f'Error while creating the table "Audio_in_movie" in MovieDB{mod}', error)

    # Subs_in_movie
    try:
        sql_query = f'''CREATE TABLE MovieDB{mod}.Subs_in_movie (
                    filmID SMALLINT UNSIGNED NOT NULL,
                    languageID TINYINT UNSIGNED NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (languageID) REFERENCES Languages(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print(f'- "Subs_in_movie" table has been created correctly in MovieDB{mod}')
    
    except sql.Error as error:
        print(f'Error while creating the table "Subs_in_movie" in MovieDB{mod}', error)

    # Genre - Categories
    try:
        sql_query = f'''CREATE TABLE MovieDB{mod}.Genre_Categories (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Category VARCHAR(15) NOT NULL UNIQUE,
                    PRIMARY KEY(id));'''

        db.execute(sql_query)
        conn.commit()
        print(f'- "Genre_Categories" table has been created correctly in MovieDB{mod}')
    
    except sql.Error as error:
        print(f'Error while creating the table "Genre_Categories" in MovieDB{mod}', error)

    # Genres
    try:
        sql_query = f'''CREATE TABLE MovieDB{mod}.Genres (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    CategoryID TINYINT UNSIGNED NOT NULL,
                    Name VARCHAR(40) NOT NULL UNIQUE,
                    PRIMARY KEY(id),
                    FOREIGN KEY (CategoryID) REFERENCES Genre_Categories(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print(f'- "Genres" table has been created correctly in MovieDB{mod}')
    
    except sql.Error as error:
        print(f'Error while creating the table "Genres" in MovieDB{mod}', error)

    # Genre_in_movie
    try:
        sql_query = f'''CREATE TABLE MovieDB{mod}.Genre_in_movie (
                    filmID SMALLINT UNSIGNED NOT NULL,
                    genreID TINYINT UNSIGNED NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (genreID) REFERENCES Genres(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print(f'- "Genre_in_movie" table has been created correctly in MovieDB{mod}')
    
    except sql.Error as error:
        print(f'Error while creating the table "Genre_in_movie" in MovieDB{mod}', error)

    # Users
    try:
        sql_query = f'''CREATE TABLE MovieDB{mod}.Users (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Name VARCHAR(20) NOT NULL UNIQUE,
                    Password VARCHAR (50) NOT NULL,
                    Email VARCHAR (40) NOT NULL UNIQUE,
                    PRIMARY KEY(id));'''

        db.execute(sql_query)
        conn.commit()
        print(f'- "Genre_Categories" table has been created correctly in MovieDB{mod}')
    
    except sql.Error as error:
        print(f'Error while creating the table "Genre_Categories" in MovieDB{mod}', error)

    db.close()
    conn.close()
    print(f"\nDisconnected from database 'MovieDB{mod}'\n")

def delete_tables(mod:str=''):
    ''' 
    - 'mod' variable is a modifier to delete the same tables in the _test database. 
    If it's empty, it does not modify the original sql queries and all the tables
    are deleted from the MovieDB database. 

    This function will be used during the development process to save time but will be deleted
    once the database is in production for safety reasons.   
    '''
    # Connect to MySQL 'MovieDB'
    [conn, db] = dbConnector.connect_to_db(mod)

    print(f"Deleting all tables from MovieDB{mod}")

    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Audio_in_movie;")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Subs_in_movie;")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Genre_in_movie;")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Genres;")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Genre_Categories;")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Main;")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Countries;")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Languages;")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Qualities;")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Storage;")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Users;")

    db.close()
    conn.close()
    print(f"\nDisconnected from database 'MovieDB{mod}'\n")