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

conn = None  # Initialize conn to avoid NameError

try:
    # Connect to the database using the parameters from the .my.cnf file
    conn = mysql.connector.connect(**db_config)
    print("Connection successful!")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Check your credentials!")
    else:
        print(f"Error: {err}")

finally:
    if conn and conn.is_connected():
        conn.close()
        print("Connection closed.")




# import mysql.connector
# from mysql.connector import errorcode

# try:
#     # Connect using the .my.cnf file (ensure the path is correct)
#     conn = mysql.connector.connect(read_default_file=r"C:\Users\saipe\OneDrive\Desktop\bigquery-project1\.my.cnf")
#     print("Connection successful!")

# except mysql.connector.Error as err:
#     if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#         print("Check your credentials!")
#     else:
#         print(f"Error: {err}")

# finally:
#     if conn and conn.is_connected():
#         conn.close()
#         print("Connection closed.")

# import mysql.connector
# from mysql.connector import errorcode
# import configparser

# # Load configuration file
# config = configparser.ConfigParser()
# config.read(r"C:\Users\saipe\OneDrive\Desktop\bigquery-project1\config.ini")

# # Extract MySQL connection parameters from the config file
# db_config = {
#     'user': config['mysql']['user'],
#     'password': config['mysql']['password'],
#     'host': config['mysql']['host'],
#     'database': config['mysql']['database']
# }

# try:
#     # Connect to the database
#     conn = mysql.connector.connect(**db_config)
#     print("Connection successful!")

# except mysql.connector.Error as err:
#     if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#         print("Check your credentials!")
#     else:
#         print(f"Error: {err}")

# finally:
#     if conn.is_connected():
#         conn.close()
#         print("Connection closed.")
