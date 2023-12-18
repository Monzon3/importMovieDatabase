''' The idea is to create the database using this file and leave it as a future reference
to look up which tables are in it, type of data within them, etc. 
The new empty database is created as 00_EmptyDatabase.db.'''
import common.dbConnector as dbConnector
import sqlite3 as sql

if __name__ == '__main__':
    # Connect with 'create_database' which is in [Paths] section from .ini file
    [conn, db] = dbConnector.connect_to_db('create_database')

    # Generate tables
    # Main table
    try:
        sql_query = '''CREATE TABLE Main (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Title TEXT NOT NULL,
                    OriginalTitle TEXT NOT NULL,
                    StorageID INTEGER NOT NULL,
                    QualityID INTEGER NOT NULL, 
                    Year INTEGER NOT NULL CHECK(Year>1880),
                    CountryID INTEGER NOT NULL,
                    Length INTEGER NOT NULL CHECK(Length>=0),
                    Director TEXT NOT NULL,
                    Screenwriter TEXT,
                    Score INTEGER Check(Score>=0 AND Score<=10),
                    Image TEXT,
                    FOREIGN KEY (StorageID) REFERENCES Storage(id)
                    ON DELETE SET NULL ON UPDATE CASCADE,
                    FOREIGN KEY (QualityID) REFERENCES Qualities(id)
                    ON DELETE SET NULL ON UPDATE CASCADE,
                    FOREIGN KEY (CountryID) REFERENCES Countries(id)
                    ON DELETE SET NULL ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print('"Main" table has been created correctly')

    except sql.Error as error:
        print('Error while creating the table "Main"', error)

    # Storage (this approach is called database normalization in sql)
    try:
        sql_query = '''CREATE TABLE Storage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Device TEXT NOT NULL UNIQUE);'''

        db.execute(sql_query)
        conn.commit()
        print('"Storage" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Storage"', error)

    # Qualities
    try:
        sql_query = '''CREATE TABLE Qualities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Quality TEXT NOT NULL UNIQUE);'''

        db.execute(sql_query)
        conn.commit()
        print('"Qualities" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Qualities"', error)

    # Languages
    try:
        sql_query = '''CREATE TABLE Languages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    LangShort TEXT NOT NULL,
                    LangComplete TEXT NOT NULL);'''

        db.execute(sql_query)
        conn.commit()
        print('"Languages" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Languages"', error)

    # Audio_in_file
    try:
        sql_query = '''CREATE TABLE Audio_in_file (
                    filmID INTEGER NOT NULL,
                    languageID INTEGER NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (languageID) REFERENCES Languages(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print('"Audio_in_file" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Audio_in_file"', error)

    # Subs_in_file
    try:
        sql_query = '''CREATE TABLE Subs_in_file (
                    filmID INTEGER NOT NULL,
                    languageID INTEGER NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (languageID) REFERENCES Languages(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print('"Subs_in_file" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Subs_in_file"', error)

    # Countries
    try:
        sql_query = '''CREATE TABLE Countries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Country TEXT NOT NULL UNIQUE);'''

        db.execute(sql_query)
        conn.commit()
        print('"Countries" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Countries"', error)

    # Genres
    try:
        sql_query = '''CREATE TABLE Genres (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    CategoryID INTEGER NOT NULL,
                    Name TEXT NOT NULL,
                    FOREIGN KEY (CategoryID) REFERENCES Genre_Categories(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print('"Genres" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Genres"', error)

    # Genre - Categories
    try:
        sql_query = '''CREATE TABLE Genre_Categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Category TEXT NOT NULL UNIQUE);'''

        db.execute(sql_query)
        conn.commit()
        print('"Genre_Categories" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Genre_Categories"', error)

    # Genre_in_file
    try:
        sql_query = '''CREATE TABLE Genre_in_file (
                    filmID INTEGER NOT NULL,
                    genreID INTEGER NOT NULL,
                    FOREIGN KEY (filmID) REFERENCES Main(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (genreID) REFERENCES Genres(id)
                    ON DELETE CASCADE ON UPDATE CASCADE);'''

        db.execute(sql_query)
        conn.commit()
        print('"Genre_in_file" table has been created correctly')
    
    except sql.Error as error:
        print('Error while creating the table "Genre_in_file"', error)

    db.close()
    conn.close()