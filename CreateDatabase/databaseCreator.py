'''
La idea es crear la base de datos utilizando este archivo y dejarlo como futura referencia
para ver qué tablas existen, tipos de datos contenidos en ellas, etc.
'''
from configparser import ConfigParser
import sqlite3 as sql

config = ConfigParser()
config.read('C:\\Users\\F36SMD0\\Desktop\\Documentos\\Mios\\database\\configuration.ini')
db_path = config.get('Paths', 'database')

# Conexión con la base de datos
conn = sql.connect(db_path)
db = conn.cursor()

# Generar tablas
# Tabla principal
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
    print('Tabla \"Películas\" creada correctamente')

except sql.Error as error:
    print('Error al crear la tabla \"Películas\"', error)

 # Disco
try:
    sql_query = ''' CREATE TABLE Disco (
                id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
                Disco VARCHAR(255) NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('Tabla \"Disco\" creada correctamente')
   
except sql.Error as error:
    print('Error al crear la tabla \"Disco\"', error)


# Calidad
try:
    sql_query = '''CREATE TABLE Calidad (
                id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
                Calidad VARCHAR(255) NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('Tabla \"Calidad\" creada correctamente')
   
except sql.Error as error:
    print('Error al crear la tabla \"Calidad\"', error)

# Idioma
try:
    sql_query = '''CREATE TABLE Idioma (
                id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
                IdiomaAbr CHAR(3) NOT NULL,
                IdiomaCompleto VARCHAR(255) NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('Tabla \"Idioma\" creada correctamente')
   
except sql.Error as error:
    print('Error al crear la tabla \"Idioma\"', error)

# Audio_en_archivo
try:
    sql_query = '''CREATE TABLE Audio_en_archivo (
                pelicula_id SMALLINT UNSIGNED NOT NULL,
                idioma_id SMALLINT UNSIGNED NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('Tabla \"Audio_en_archivo\" creada correctamente')
   
except sql.Error as error:
    print('Error al crear la tabla \"Audio_en_archivo\"', error)

# Subs_en_archivo
try:
    sql_query = '''CREATE TABLE Subs_en_archivo (
                pelicula_id SMALLINT UNSIGNED NOT NULL,
                idioma_id SMALLINT UNSIGNED NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('Tabla \"Subs_en_archivo\" creada correctamente')
   
except sql.Error as error:
    print('Error al crear la tabla \"Subs_en_archivo\"', error)

# Pais
try:
    sql_query = '''CREATE TABLE Pais (
                id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
                Pais VARCHAR(255) NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('Tabla \"Pais\" creada correctamente')
   
except sql.Error as error:
    print('Error al crear la tabla \"Pais\"', error)

# Género - plantilla
try:
    sql_query = '''CREATE TABLE Genero (
                id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
                Genero VARCHAR(255));'''

    db.execute(sql_query)
    conn.commit()
    print('Tabla \"Genero\" creada correctamente')
   
except sql.Error as error:
    print('Error al crear la tabla \"Genero\"', error)

# Género_en_archivo
try:
    sql_query = '''CREATE TABLE Genero_en_archivo (
                pelicula_id SMALLINT UNSIGNED NOT NULL,
                genero_id SMALLINT UNSIGNED NOT NULL);'''

    db.execute(sql_query)
    conn.commit()
    print('Tabla \"Genero_en_archivo\" creada correctamente')
   
except sql.Error as error:
    print('Error al crear la tabla \"Genero_en_archivocls\"', error)

db.close()
conn.close()