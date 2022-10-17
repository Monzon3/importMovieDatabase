'''
The idea is to create the database using this file and leave it as a future reference
to look up which tables exists, type of data within them, etc.
'''
from configparser import ConfigParser
import sqlite3 as sql

config = ConfigParser()
config.read('C:\\Users\\F36SMD0\\Desktop\\Documentos\\Mios\\database\\configuration.ini')
db_path = config.get('Paths', 'database')

# Connect with the database
conn = sql.connect(db_path)
db = conn.cursor()

# Generate tables
# Main table
try:
    sql_query = '''CREATE TABLE Peliculas (
                id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
                Titulo VARCHAR(255) NOT NULL,
                TituloOriginal VARCHAR(255) NOT NULL,
                Disco TINYINT UNSIGNED NOT NULL,
                Calidad TINYINT UNSIGNED NOT NULL, 
                Year SMALLINT UNSIGNED NOT NULL,
                Pais VARCHAR(255) NOT NULL,
                Duracion SMALLINT UNSIGNED NOT NULL,
                Director VARCHAR(255) NOT NULL,
                Guion VARCHAR(512) NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('\"Películas\" table has been created correctly')

except sql.Error as error:
    print('Error while creating the table \"Películas\"', error)

 # Disco
try:
    sql_query = ''' CREATE TABLE Disco (
                id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
                Disco VARCHAR(255) NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('\"Disco\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Disco\"', error)


# Calidad
try:
    sql_query = '''CREATE TABLE Calidad (
                id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
                Calidad VARCHAR(255) NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('\"Calidad\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Calidad\"', error)

# Idioma
try:
    sql_query = '''CREATE TABLE Idioma (
                id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
                IdiomaAbr CHAR(3) NOT NULL,
                IdiomaCompleto VARCHAR(255) NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('\"Idioma\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Idioma\"', error)

# Audio_in_file
try:
    sql_query = '''CREATE TABLE Audio_in_file (
                pelicula_id SMALLINT UNSIGNED NOT NULL,
                idioma_id SMALLINT UNSIGNED NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('\"Audio_in_file\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Audio_in_file\"', error)

# Subs_en_archivo
try:
    sql_query = '''CREATE TABLE Subs_in_file (
                pelicula_id SMALLINT UNSIGNED NOT NULL,
                idioma_id SMALLINT UNSIGNED NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('\"Subs_in_file\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Subs_in_file\"', error)

# Pais
try:
    sql_query = '''CREATE TABLE Pais (
                id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
                Pais VARCHAR(255) NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('\"Pais\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Pais\"', error)

# Género - plantilla
try:
    sql_query = '''CREATE TABLE Genero (
                id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
                Genero VARCHAR(255));'''

    db.execute(sql_query)
    conn.commit()
    print('\"Genero\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Genero\"', error)

# Género_en_archivo
try:
    sql_query = '''CREATE TABLE Genero_en_archivo (
                pelicula_id SMALLINT UNSIGNED NOT NULL,
                genero_id SMALLINT UNSIGNED NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('\"Genero_in_file\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Genero_in_file\"', error)

db.close()
conn.close()