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
LOG_FILE = "/home/odoo/Documents/Wibtec/2086_seed_data/dex_order/dex_order_log.csv"
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
dbname = "v16_pmx_test"

# Define directory 
DIR_PATH = "/home/odoo/Documents/Wibtec/2086_seed_data/dex_order/file_chunks/"
csv_files_name_list = os.listdir(DIR_PATH)
csv_files_name_list.sort()

try:
    conn = psycopg2.connect(dbname=dbname)
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
            dex_order_data = []
            dex_order_vals = {}
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
                        logging.info(f"Processing file {file_name} : Row Number {num} : Order Record ID {current_row_id}")
                        cur.execute(f"SELECT * FROM dex_order WHERE id = {current_row_id}")
                        existing_dex_order_record = cur.fetchone()

                        if not existing_dex_order_record:
                            dex_order_vals = {
                                "active": True,
                                "id": row["id"],
                                "market": row["market"] if row["market"] else None, # Char
                                "custom_order_id": row["custom_order_id"] if row["custom_order_id"] else None, #Char
                                # "username": row["username"] if row["username"] #Char : blank
                                "pmx_id": row["pmx_id"] if row["pmx_id"] else None, #
                                "account_id": row["account_id"] if row["account_id"] else None, #char
                                "created_at": row["created_at"] if row["created_at"] else None, #magic field datetime
                                # "source_currency": row["source_currency"] if row["source_currency"] else None,     #Not in dex.order model
                                "price": row["price"] if row["price"] else None,     #float
                                "size": row["size"] if row["size"] else None,     #float
                                "side": row["side"] if row["side"] else None,     #Char
                                # "trade_type": row["trade_type"] if row["trade_type"] else None,     #Not in dex order model(char)
                                "status": row["status"] if row["status"] else None,     #Char
                                # "fs_client_id": row["fs_client_id"] if row["fs_client_id"] else None,     #Blank(M2O)
                                # "country_of_residence": row["country_of_residence"] if row["country_of_residence"] else None,     #Blank(M2O)
                                # "company_id": row["company_id"] if row["company_id"] else None,     #Blank(M2O)
                                "client_pmx_id": row["client_pmx_id"] if row["client_pmx_id"] else None,     #Blank(Char)(required)
                                "eur_value": row["eur_value"] if row["eur_value"] else None,     #Blank(Float)
                            }
                        print(dex_order_vals)
                        dex_order_data.append(dex_order_vals)
                        if dex_order_data:
                            columns = dex_order_data[0].keys()
                            values = [tuple(item.get(column) for column in columns)for item in dex_order_data]
                            insert_query = sql.SQL("INSERT INTO dex_order ({}) VALUES ({})").format(
                                sql.SQL(", ").join(map(sql.Identifier, columns)),
                                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
                            )
                            dex_order_data = []
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
