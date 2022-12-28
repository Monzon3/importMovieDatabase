from configparser import ConfigParser
import sqlite3 as sql
import pandas as pd

config = ConfigParser()
config.read('./config/configuration.ini')

# Configure connection to database
db_path = config.get('Paths', 'test_database')
conn = sql.connect(db_path)
print('Conexión correcta con la base de datos de películas')
db = conn.cursor()

sql_query = "SELECT * FROM Main"
db.execute(sql_query)
record = db.fetchall()
#print(record)

data = pd.read_sql_query('SELECT * FROM Main', conn)
print(data)

# Close cursor and connector
db.close()
conn.close()
print('Conexión con la base de datos terminada')