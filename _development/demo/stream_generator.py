import time
import random

import psycopg2


table_name = 'demo_table'


def insert(connection, cursor):
    cursor.execute(f'INSERT INTO {table_name} VALUES (round(random() * 10), round(random() * 100), random() * 10)')
    connection.commit()


def update(connection, cursor):
    cursor.execute(f'SELECT * FROM {table_name} LIMIT 1')
    row = cursor.fetchall()[0]
    cursor.execute(f'''UPDATE {table_name} SET a = 1000, b = {row[1] * 100}, c = {row[2] * 100} 
                                           WHERE a = {row[0]} AND b = {row[1]} AND c = {row[2]}''')
    connection.commit()


def delete(connection, cursor):
    cursor.execute(f'SELECT * FROM {table_name} LIMIT 1')
    row = cursor.fetchall()[0]
    cursor.execute(f'DELETE FROM {table_name} WHERE a = {row[0]} AND b = {row[1]} AND c = {row[2]}')
    connection.commit()



connection = psycopg2.connect(dbname='postgres', user='postgres', password='postgres', host='localhost', port=5432)
cursor = connection.cursor()
cursor.execute(f'TRUNCATE TABLE {table_name}')
connection.commit()

#while True:
for j in range(1000):
    for i in range(10):
        insert(connection, cursor)
    update(connection, cursor)
    delete(connection, cursor)
    time.sleep(3)

