import mysql.connector
from mysql.connector import errorcode
import configparser

# Load configuration file using configparser
config = configparser.ConfigParser()
config.read(r"C:\Users\saipe\OneDrive\Desktop\bigquery-project1\config.ini")

# Extract MySQL connection parameters from the config file
db_config = {
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'host': config['mysql']['host'],
    'database': config['mysql']['database']
}

# Connect to the MySQL database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# SQL query (using triple quotes for better readability)
query = """
    SELECT year, id, city_name
    FROM oscar_sql.city_house_price
    LIMIT 2;
"""

# Execute the query
cursor.execute(query)

# Fetch results and print them
for (year, id, city_name) in cursor:
    print(year, id, city_name)

# Close the connection
cursor.close()
conn.close()
