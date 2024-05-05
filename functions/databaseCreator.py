''' The idea is to create the database using this file and leave it as a future reference
to look up which tables are in it, type of data within them, etc.

To create all these tables, the databases MovieDB and MovieDB_test should exist and
since the script will be accessing the database with the user 'admin', this user should exist
and have the privileges for all actions.'''
import common.dbConnector as dbConnector
import pymysql as sql

def create_tables(mod:str=""):
    ''' 
    - 'mod' variable is a modifier to create the same tables in the MovieDB_test database. 
    If it's empty, it does not modify the original sql queries and all the tables
    are created in the MovieDB database.
    
    - mod: Can only be empty or '_test' to work properly.'''

    # Connect to MySQL
    [conn, db] = dbConnector.connect_to_db(mod)

    print(f"2. Generating all needed tables in 'MovieDB{mod}' database.")

    # Generate tables
    # Storage
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Storage (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Name VARCHAR(20) NOT NULL UNIQUE,
                    PRIMARY KEY(id));"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Storage' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Storage' in MovieDB{mod}", error)

    # Qualities
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Qualities (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Name VARCHAR(10) NOT NULL UNIQUE,
                    PRIMARY KEY(id));"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Qualities' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Qualities' in MovieDB{mod}", error)

    # Countries
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Countries (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Name VARCHAR(25) NOT NULL UNIQUE,
                    PRIMARY KEY(id));"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Countries' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Countries' in MovieDB{mod}", error)

    # Directors
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Directors (
                    id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Name VARCHAR(45) NOT NULL UNIQUE,
                    CountryID TINYINT UNSIGNED,
                    PRIMARY KEY(id),
                    FOREIGN KEY (CountryID) REFERENCES Countries(id)
                    ON DELETE SET NULL ON UPDATE CASCADE);"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Directors' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Directors' in MovieDB{mod}", error)

    # Main table
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Main (
                    id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Title VARCHAR(100) NOT NULL,
                    INDEX idxTitle (Title),
                    OriginalTitle VARCHAR(100) NOT NULL,
                    INDEX idxOrigTitle (OriginalTitle),
                    StorageID TINYINT UNSIGNED,
                    QualityID TINYINT UNSIGNED, 
                    Year SMALLINT UNSIGNED NOT NULL CHECK(Year>1880 AND Year<2100),
                    CountryID TINYINT UNSIGNED,
                    Length SMALLINT UNSIGNED DEFAULT (0),
                    Screenplay VARCHAR(300),
                    Score TINYINT UNSIGNED Check(Score<=10 AND Score>=0) DEFAULT (0),
                    Image VARCHAR(120),
                    PRIMARY KEY(id),
                    FOREIGN KEY (StorageID) REFERENCES Storage(id)
                    ON DELETE SET NULL ON UPDATE CASCADE,
                    FOREIGN KEY (QualityID) REFERENCES Qualities(id)
                    ON DELETE SET NULL ON UPDATE CASCADE,
                    FOREIGN KEY (CountryID) REFERENCES Countries(id)
                    ON DELETE SET NULL ON UPDATE CASCADE);"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Main' table has been created correctly in MovieDB{mod}")

    except sql.Error as error:
        print(f"Error while creating the table 'Main' in MovieDB{mod}", error)

    # Languages
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Languages (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    LangShort VARCHAR(6) NOT NULL UNIQUE,
                    LangComplete VARCHAR(15) NOT NULL UNIQUE,
                    PRIMARY KEY(id));"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Languages' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Languages' in MovieDB{mod}", error)

    # Audio_in_movie
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Audio_in_movie (
                    filmID SMALLINT UNSIGNED NOT NULL,
                    languageID TINYINT UNSIGNED NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (languageID) REFERENCES Languages(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Audio_in_movie' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Audio_in_movie' in MovieDB{mod}", error)

    # Subs_in_movie
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Subs_in_movie (
                    filmID SMALLINT UNSIGNED NOT NULL,
                    languageID TINYINT UNSIGNED NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (languageID) REFERENCES Languages(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Subs_in_movie' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Subs_in_movie' in MovieDB{mod}", error)

    # Director_in_movie
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Director_in_movie (
                    filmID SMALLINT UNSIGNED NOT NULL,
                    directorID SMALLINT UNSIGNED NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (directorID) REFERENCES Directors(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Director_in_movie' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Director_in_movie' in MovieDB{mod}", error)

    # Genre - Categories
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Genre_Categories (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Name VARCHAR(15) NOT NULL UNIQUE,
                    PRIMARY KEY(id));"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Genre_Categories' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Genre_Categories' in MovieDB{mod}", error)

    # Genres
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Genres (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Name VARCHAR(40) NOT NULL UNIQUE,
                    CategoryID TINYINT UNSIGNED NOT NULL,
                    PRIMARY KEY(id),
                    FOREIGN KEY (CategoryID) REFERENCES Genre_Categories(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Genres' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Genres' in MovieDB{mod}", error)

    # Genre_in_movie
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Genre_in_movie (
                    filmID SMALLINT UNSIGNED NOT NULL,
                    genreID TINYINT UNSIGNED NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (genreID) REFERENCES Genres(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Genre_in_movie' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Genre_in_movie' in MovieDB{mod}", error)

    # User_ranks
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.User_ranks (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Name VARCHAR(10) NOT NULL UNIQUE,
                    PRIMARY KEY(id));"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'User_ranks' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'User_ranks' in MovieDB{mod}", error)

    # Insert values into User_ranks
    try:
        sql_query = "INSERT INTO MovieDB{mod}.User_ranks (Name) Values ('admin'), ('powerUser'), ('user');" 

    except sql.Error as error:
        print(f"Error while adding default values into 'User_ranks' in MovieDB{mod}", error)

    # Users
    try:
        sql_query = f"""CREATE TABLE MovieDB{mod}.Users (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Name VARCHAR(20) NOT NULL UNIQUE,
                    Password VARCHAR(50) NOT NULL,
                    Email VARCHAR(40) NOT NULL UNIQUE,
                    RankID TINYINT UNSIGNED NOT NULL DEFAULT (1),
                    Disabled BOOL DEFAULT (FALSE),
                    PRIMARY KEY(id),
                    FOREIGN KEY (RankID) REFERENCES User_ranks(id));"""

        db.execute(sql_query)
        conn.commit()
        print(f"\t- 'Users' table has been created correctly in MovieDB{mod}")
    
    except sql.Error as error:
        print(f"Error while creating the table 'Users' in MovieDB{mod}", error)

    db.close()
    conn.close()
    print(f"\n-- Disconnected from database 'MovieDB{mod}' --\n")

def delete_tables(mod:str=""):
    ''' 
    - 'mod' variable is a modifier to delete the same tables in the MovieDB_test database. 
    If it's empty, it does not modify the original sql queries and all the tables
    are deleted from the MovieDB database. 

    This function will be used during the development process to save time but will be deleted
    once the database is in production for safety reasons. 

    - mod: Can only be empty or '_test' to work properly.'''
    # Connect to MySQL database
    [conn, db] = dbConnector.connect_to_db(mod)

    print(f"1. Deleting all tables from 'MovieDB{mod}' database.")

    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Audio_in_movie;")
    print(f"\t- 'Audio_in_movie' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Subs_in_movie;")
    print(f"\t- 'Subs_in_movie' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Director_in_movie;")
    print(f"\t- 'Director_in_movie' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Genre_in_movie;")
    print(f"\t- 'Genre_in_movie' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Genres;")
    print(f"\t- 'Genres' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Genre_Categories;")
    print(f"\t- 'Genre_Categories' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Main;")
    print(f"\t- 'Main' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Directors;")
    print(f"\t- 'Directors' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Countries;")
    print(f"\t- 'Countries' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Languages;")
    print(f"\t- 'Languages' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Qualities;")
    print(f"\t- 'Qualities' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Storage;")
    print(f"\t- 'Storage' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.Users;")
    print(f"\t- 'Users' table has been deleted correctly from MovieDB{mod}")
    db.execute(f"DROP TABLE IF EXISTS MovieDB{mod}.User_ranks;")
    print(f"\t- 'User_ranks' table has been deleted correctly from MovieDB{mod}")

    db.close()
    conn.close()
    print(f"\n-- Disconnected from database 'MovieDB{mod}' --\n")