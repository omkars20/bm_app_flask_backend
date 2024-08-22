# database.py
import mysql.connector

def get_db_connection():
    connection = mysql.connector.connect(
        user='your user name',
        password='your password',
        host='host name',  
        database='database name',   
    )
    return connection
