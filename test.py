# test_db_connection.py
import mysql.connector

try:
    connection = mysql.connector.connect(
        user='wiomsudo',
        password='wiomsudo',
        host='34.131.53.25',
        database='wiomlabs',
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
