import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()   # load .env variables

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# create MySQL connection
conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT,
    database=DB_NAME
)

# test connection
if conn.is_connected():
    print("✅ Connected to MySQL using mysql.connector")


# close connection (optional here)
conn.close()
