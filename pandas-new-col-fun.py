import mysql.connector
from mysql.connector import errorcode
import configparser
import pandas as pd
import os

# Get the current working directory and define file path
cur_path = os.getcwd()
file = 'House_price_tf_fuc.csv'
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
    SELECT year, id, city_name 
    FROM oscar_sql.city_house_price
    WHERE year BETWEEN 2004 AND 2025;
"""

# Function to categorize year as "even" or "odd"
def duration(d):
    if d % 2 == 0:
        return "even"
    elif d % 2 != 0:
        return "odd"
    else:
        return "No num"

# Fetch data from the database
try:
    df = pd.read_sql(query, conn)
    df["new_col"] = df['year'].apply(duration)
    print("Data fetched successfully.")
except Exception as e:
    print(f"Error fetching data: {e}")
    conn.close()
    exit()

# Save the dataframe to CSV
try:
    df.to_csv(file_path, index=False)
    print(f"File saved successfully to {file_path}")
except Exception as e:
    print(f"Error saving file: {e}")

# Close the connection
conn.close()
print("Database connection closed.")
