# test_db_connection.py
import mysql.connector

try:
    connection = mysql.connector.connect(
        user='your user name',
        password='your password',
        host='your host',
        database='your database name',
    )
    if connection.is_connected():
        print("Connection successful!")
    else:
        print("Connection failed.")

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if connection.is_connected():
        connection.close()
