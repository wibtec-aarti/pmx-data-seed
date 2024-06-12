from datetime import datetime, date
from psycopg2 import sql
import pandas as pd
import psycopg2
import logging
import random
import string
import time
import csv
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

# Record the script start time for file
script_start_time = time.time()


# Path to the CSV file for logging errors
LOG_FILE = "/home/odoo/Documents/Wibtec/2086_seed_data/position/position_log.csv"
LOG_FILE_HEADER = ["file_name", "chunk_range", "record_id", "error"]

# Function to write error records to CSV
def write_error_to_csv(file_name, chunk_range, record_id, error_message):
    with open(LOG_FILE, mode='a',newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=LOG_FILE_HEADER)
        file_exists = os.path.exists(LOG_FILE)
        if not file_exists:
            writer.writeheader()
        writer.writerow({"file_name": file_name, "chunk_range": chunk_range, "record_id": record_id, "error": error_message})

#Database connection parameter 
dbname = "Master"
user = "odoo"
password = "AVNS_0byMv122u7uygOUkk4A"
host = "private-test-pmx-coresystems-do-user-2412463-0.c.db.ondigitalocean.com"
port = "25060"

# Define directory 
DIR_PATH = "/home/odoo/Documents/Wibtec/2086_seed_data/position/file_chunks/"
csv_files_name_list = os.listdir(DIR_PATH)
csv_files_name_list.sort()
conn = None

try:
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    logging.info("\nConnection established successfully!\n")
    cur = conn.cursor()

    for file_name in csv_files_name_list:
        file_dir_path = "%s/%s" % (DIR_PATH, file_name)
        logging.info(f"Processing file: {file_name}")
        
        # Record the start time for file
        start_time = time.time()

        # Read data from CSV and insert into the database
        with open(file_dir_path, "r", encoding="utf-8") as file:
            reader = list(csv.DictReader(file))
            position_data = []
            position_vals = {}
            batch_size = 50
            num = 0
            counter = 0
            for index in range(0, len(reader), batch_size):
                batch = reader[index : index + batch_size]
                counter = index
                for row in batch:
                    try:
                        num += 1
                        current_row_id = row.get('id')
                        logging.info(f"Processing file {file_name} : Row Number {num} : position Record ID {current_row_id}")
                        cur.execute(f"SELECT * FROM position WHERE id = {current_row_id}")
                        existing_position_record = cur.fetchone()

                        if not existing_position_record:
                            position_vals = {
                                "active": True,
                                "id": row["id"],
                                "pmx_id": row["pmx_id"] if row["pmx_id"] else None, # (char: Required)
                                "client_pmx_id": row["client_pmx_id"] if row["client_pmx_id"] else None, #(float)
                                # "account_id": row["account_id"] if row["account_id"] else None, #Blank (Char)
                                "eur_value": row["eur_value"] if row["eur_value"] else None, #(Float)
                                # "amount_currency": row["amount_currency"] if row["amount_currency"] else None, #Blank (monetry)
                                # "fs_client_id": row["fs_client_id"] if row["fs_client_id"] else None, #(Blank: M2O)
                                # "company_id": row["company_id"] if row["company_id"] else None, #(Blank: M2O)
                                # "currency_id": row["currency_id"] if row["currency_id"] else None, #(Blank: M2O)
                                "instrument": row["instrument"] if row["instrument"] else None, #(char)
                                # "market": row["market"] if row["market"] else None, #(blank: char)
                                # "size": row["size"] if row["size"] else None, #(blank: float)
                                "position_date": row["position_date"] if row["position_date"] else None, #(datetime)
                            }
                        print(position_vals)
                        position_data.append(position_vals)
                        print("position_data>>>", position_data)
                        if position_data:
                            columns = position_data[0].keys()
                            values = [tuple(item.get(column) for column in columns)for item in position_data]
                            insert_query = sql.SQL("INSERT INTO position ({}) VALUES ({})").format(
                                sql.SQL(", ").join(map(sql.Identifier, columns)),
                                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
                            )
                            position_data = []
                            cur.executemany(insert_query, values)
                            conn.commit()
                    except psycopg2.Error as error:
                        print("error >>>", error)
                        conn.rollback()
                        error_message = f"Error: Unable to import data from file {file_name}, row {num}. Error: {error}"
                        write_error_to_csv(file_name, f"Row Number: {num}", f"{current_row_id}", error_message)
                        continue
                write_error_to_csv(file_name, f"{counter+1}-{num}", 'N/A', 'N/A')
        # Calculate End time
        end_time = time.time()
        execution_duration = end_time - start_time
        execution_duration_minutes = execution_duration / 60
        logging.info(f"\n\n\n\n {file_name} Time taken : {execution_duration:.2f} minutes")
        # os.remove(file_dir_path)
except psycopg2.Error as e:
    logging.error(f"Database connection failed: {e}")
finally:
    if conn is not None:
        conn.commit()
        cur.close()
        conn.close()
        logging.info("\n\nConnection closed.")

    # Calculate End time
    script_end_time = time.time()
    script_execution_duration = script_end_time - script_start_time
    script_execution_duration_minutes = script_execution_duration / 60
    logging.info(f"\n\n\n\n Script Time taken : {script_execution_duration_minutes} minutes")
