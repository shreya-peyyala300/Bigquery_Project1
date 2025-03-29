import mysql.connector
from mysql.connector import errorcode
import configparser
import pandas as pd
import os

cur_path=os.getcwd()
file= 'House_price_tf.csv'
file_path=os.path.join(cur_path,'data_files', file)
print(file_path)

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
    SELECT year, id, city_name ,
    CASE 
    WHEN year % 2 = 0 THEN "even"
    WHEN year % 2 != 0 THEN "odd"
    END AS even_odd_year
    FROM oscar_sql.city_house_price
    where year between 2004 and 2025;
"""

df=pd.read_sql(query,conn)

yr_2005=df['year']==2005

df.to_csv(file_path,index=False)
# df['year'].to_csv(file_path,index=False)

# # Close the connection
# cursor.close()
conn.close()
