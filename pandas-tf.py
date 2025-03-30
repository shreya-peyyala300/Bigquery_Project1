# import mysql.connector
# from mysql.connector import errorcode
# import configparser
# import pandas as pd
# import os

# # Get the current working directory and define file path
# cur_path = os.getcwd()
# file = 'House_price_tf_fuc__1.csv'
# folder_path = os.path.join(cur_path, 'data_files')

# # Check if the 'data_files' directory exists, if not, create it
# if not os.path.exists(folder_path):
#     os.makedirs(folder_path)

# file_path = os.path.join(folder_path, file)
# print(f"Saving file to: {file_path}")

# # Load configuration file using configparser
# config = configparser.ConfigParser()
# config.read(r"C:\Users\saipe\OneDrive\Desktop\bigquery-project1\config.ini")

# # Extract MySQL connection parameters from the config file
# db_config = {
#     'user': config['mysql']['user'],
#     'password': config['mysql']['password'],
#     'host': config['mysql']['host'],
#     'database': config['mysql']['database']
# }

# # Connect to the MySQL database
# try:
#     conn = mysql.connector.connect(**db_config)
#     print("Connected to the database.")
# except mysql.connector.Error as err:
#     print(f"Error: {err}")
#     exit()

# # SQL query (using triple quotes for better readability)
# query = """
#     SELECT * 
#     FROM oscar_sql.city_house_price;
# """

# df = pd.read_sql(query, conn)
# df.set_index('id',inplace=True)
# df=df.stack().reset_index()
# df.to_csv(file_path, index=False)

# conn.close()
# print("Database connection closed.")
import mysql.connector
from mysql.connector import errorcode
import configparser
import pandas as pd
import os

# Get the current working directory and define file path
cur_path = os.getcwd()
file = 'House_price_tf_fuc__1.csv'
folder_path = os.path.join(cur_path, 'data_files')

# Check if the 'data_files' directory exists, if not, create it
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

file_path = os.path.join(folder_path, file)
print(f"Saving file to: {file_path}")

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
try:
    conn = mysql.connector.connect(**db_config)
    print("Connected to the database.")
except mysql.connector.Error as err:
    print(f"Error: {err}")
    exit()

# SQL query (using triple quotes for better readability)
query = """
    SELECT * 
    FROM oscar_sql.city_house_price;
"""

# Fetch the data from the database
df = pd.read_sql(query, conn)

# Optionally set the index if required (not using stack)
df.set_index('id', inplace=True)

# Save the data to CSV
df.to_csv(file_path, index=True)  # Saving with the index (ID) as part of the CSV

# Close the connection
conn.close()
print("Database connection closed.")
