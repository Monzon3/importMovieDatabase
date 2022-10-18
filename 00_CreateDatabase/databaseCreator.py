'''
The idea is to create the database using this file and leave it as a future reference
to look up which tables exists, type of data within them, etc.
'''
from configparser import ConfigParser
import sqlite3 as sql

config = ConfigParser()
config.read('C:\\MisCosas\\Documentos\\MovieDatabase\\configuration.ini')
db_path = config.get('Paths', 'create_database')

print(db_path)

# Connect with the database
conn = sql.connect(db_path)
db = conn.cursor()

# Generate tables
# Main table
try:
    sql_query = '''CREATE TABLE Main (
                id INTEGER NOT NULL,
                Titulo TEXT NOT NULL,
                TituloOriginal TEXT NOT NULL,
                Disco INTEGER NOT NULL,
                Calidad INTEGER NOT NULL, 
                Year INTEGER NOT NULL,
                Pais TEXT NOT NULL,
                Duracion INTEGER NOT NULL,
                Director TEXT NOT NULL,
                Guion TEXT NOT NULL,
                PRIMARY KEY (id));'''

    db.execute(sql_query)
    conn.commit()
    print('\"Películas\" table has been created correctly')

except sql.Error as error:
    print('Error while creating the table \"Películas\"', error)

 # Disco
try:
    sql_query = '''CREATE TABLE Disco (
                id INTEGER NOT NULL,
                Disco TEXT NOT NULL,
                PRIMARY KEY (id));'''

    db.execute(sql_query)
    conn.commit()
    print('\"Disco\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Disco\"', error)

# Calidad
try:
    sql_query = '''CREATE TABLE Calidad (
                id INTEGER NOT NULL,
                Calidad TEXT NOT NULL,
                PRIMARY KEY (id));'''

    db.execute(sql_query)
    conn.commit()
    print('\"Calidad\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Calidad\"', error)

# Idioma
try:
    sql_query = '''CREATE TABLE Idioma (
                id INTEGER NOT NULL,
                IdiomaAbreviado TEXT NOT NULL,
                IdiomaCompleto TEXT NOT NULL,
                PRIMARY KEY (id));'''

    db.execute(sql_query)
    conn.commit()
    print('\"Idioma\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Idioma\"', error)

# Audio_in_file
try:
    sql_query = '''CREATE TABLE Audio_in_file (
                pelicula_id INTEGER NOT NULL,
                idioma_id INTEGER NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('\"Audio_in_file\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Audio_in_file\"', error)

# Subs_in_file
try:
    sql_query = '''CREATE TABLE Subs_in_file (
                pelicula_id INTEGER NOT NULL,
                idioma_id INTEGER NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('\"Subs_in_file\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Subs_in_file\"', error)

# Pais
try:
    sql_query = '''CREATE TABLE Pais (
                id INTEGER NOT NULL,
                Nombre INTEGER NOT NULL,
                PRIMARY KEY (id));'''

    db.execute(sql_query)
    conn.commit()
    print('\"Pais\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Pais\"', error)

# Género - plantilla
try:
    sql_query = '''CREATE TABLE Genero (
                id INTEGER NOT NULL,
                Categoria TEXT NOT NULL,
                Nombre TEXT NOT NULL,
                PRIMARY KEY (id));'''

    db.execute(sql_query)
    conn.commit()
    print('\"Genero\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Genero\"', error)

# Género_in_file
try:
    sql_query = '''CREATE TABLE Genero_en_archivo (
                pelicula_id INTEGER NOT NULL,
                genero_id INTEGER NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('\"Genero_in_file\" table has been created correctly')
   
except sql.Error as error:
    print('Error while creating the table \"Genero_in_file\"', error)

db.close()
conn.close()