import mysql.connector
from mysql.connector import errorcode
import configparser
import pandas as pd

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


# SQL query (using triple quotes for better readability)
query = """
    SELECT year, id, city_name
    FROM oscar_sql.city_house_price
    LIMIT 2;
"""

df=pd.read_sql(query,conn)
print(df.head())

print(df.dtypes)

# # Close the connection
# cursor.close()
conn.close()
