# database.py
import mysql.connector

def get_db_connection():
    connection = mysql.connector.connect(
        user='wiomsudo',
        password='wiomsudo',
        host='34.131.53.25',  # Ensure this IP address is correct
        database='wiomlabs',   # Ensure this database exists
    )
    return connection
