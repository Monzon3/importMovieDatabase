''' The idea is to create the database using this file and leave it as a future reference
to look up which tables are in it, type of data within them, etc. 
The new empty database is created as 00_EmptyDatabase.db.'''
import common.dbConnector as dbConnector
import pymysql as sql

if __name__ == '__main__':
    # Connect to MySQL 'MovieDB'
    [conn, db] = dbConnector.connect_to_db()

    # Generate tables
    # Storage (this approach is called database normalization in sql)
    try:
        sql_query = '''CREATE TABLE MovieDB.Storage (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Device VARCHAR(20) NOT NULL UNIQUE,
                    PRIMARY KEY(id));'''

        db.execute(sql_query)
        conn.commit()
        print('- "Storage" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Storage"', error)

    # Qualities
    try:
        sql_query = '''CREATE TABLE MovieDB.Qualities (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Quality VARCHAR(10) NOT NULL UNIQUE,
                    PRIMARY KEY(id));'''

        db.execute(sql_query)
        conn.commit()
        print('- "Qualities" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Qualities"', error)

    # Countries
    try:
        sql_query = '''CREATE TABLE MovieDB.Countries (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Country VARCHAR(25) NOT NULL UNIQUE,
                    PRIMARY KEY(id));'''

        db.execute(sql_query)
        conn.commit()
        print('- "Countries" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Countries"', error)

    # Main table
    try:
        sql_query = '''CREATE TABLE MovieDB.Main (
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
        print('- "Main" table has been created correctly')

    except sql.Error as error:
        print('Error while creating the table "Main"', error)

    # Languages
    try:
        sql_query = '''CREATE TABLE MovieDB.Languages (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    LangShort VARCHAR(6) NOT NULL UNIQUE,
                    LangComplete VARCHAR(15) NOT NULL UNIQUE,
                    PRIMARY KEY(id));'''

        db.execute(sql_query)
        conn.commit()
        print('- "Languages" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Languages"', error)

    # Audio_in_file
    try:
        sql_query = '''CREATE TABLE MovieDB.Audio_in_file (
                    filmID SMALLINT UNSIGNED NOT NULL,
                    languageID TINYINT UNSIGNED NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (languageID) REFERENCES Languages(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print('- "Audio_in_file" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Audio_in_file"', error)

    # Subs_in_file
    try:
        sql_query = '''CREATE TABLE MovieDB.Subs_in_file (
                    filmID SMALLINT UNSIGNED NOT NULL,
                    languageID TINYINT UNSIGNED NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (languageID) REFERENCES Languages(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print('- "Subs_in_file" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Subs_in_file"', error)

    # Genre - Categories
    try:
        sql_query = '''CREATE TABLE MovieDB.Genre_Categories (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    Category VARCHAR(15) NOT NULL UNIQUE,
                    PRIMARY KEY(id));'''

        db.execute(sql_query)
        conn.commit()
        print('- "Genre_Categories" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Genre_Categories"', error)

    # Genres
    try:
        sql_query = '''CREATE TABLE MovieDB.Genres (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    CategoryID TINYINT UNSIGNED NOT NULL,
                    Name VARCHAR(40) NOT NULL UNIQUE,
                    PRIMARY KEY(id),
                    FOREIGN KEY (CategoryID) REFERENCES Genre_Categories(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print('- "Genres" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Genres"', error)

    # Genre_in_file
    try:
        sql_query = '''CREATE TABLE MovieDB.Genre_in_file (
                    filmID SMALLINT UNSIGNED NOT NULL,
                    genreID TINYINT UNSIGNED NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (genreID) REFERENCES Genres(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print('- "Genre_in_file" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Genre_in_file"', error)

    db.close()
    conn.close()
    print("\nDisconnected from database 'MovieDB'\n")