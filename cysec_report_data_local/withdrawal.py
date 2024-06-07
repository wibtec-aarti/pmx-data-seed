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
LOG_FILE = "/home/odoo/Documents/Wibtec/2086_seed_data/withdrawal/withdrawal_log.csv"
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
DIR_PATH = "/home/odoo/Documents/Wibtec/2086_seed_data/withdrawal/file_chunks/"
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
            withdrawal_data = []
            withdrawal_vals = {}
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
                        logging.info(f"Processing file {file_name} : Row Number {num} : withdrawal Record ID {current_row_id}")
                        cur.execute(f"SELECT * FROM withdrawal WHERE id = {current_row_id}")
                        existing_withdrawal_record = cur.fetchone()

                        if not existing_withdrawal_record:
                            withdrawal_vals = {
                                "id": row["id"],
                                "account_id": row["account_id"] if row["account_id"] else None, # (char
                                "usd_value": row["usd_value"] if row["usd_value"] else None, #(float)
                                # "amount_currency": row["amount_currency"] if row["amount_currency"] else None, #Blank (monetry)
                                # "approved_withdrawal": row["approved_deposit"] if row["approved_deposit"] else None, #Blank (Boolean)
                                "bank": row["bank"] if row["bank"] else None, #(char)
                                # "fs_client_id": row["fs_client_id"] if row["fs_client_id"] else None, #(Blank: M2O)
                                # "company_id": row["company_id"] if row["company_id"] else None, #blank (M2o)
                                # "created_at": row["created_at"] if row["created_at"] else None, #(Date time)
                                # "currency_id": row["currency_id"] if row["currency_id"] else None, #blank (M2o)
                                # "destination_srn": row["destination_srn"] if row["destination_srn"] else None, #blank (M2o)
                                # "error": row["error"] if row["error"] else None, #char
                                # "fee": row["fee"] if row["fee"] else None, #char not in model
                                "fee_instrument": row["fee_instrument"] if row["fee_instrument"] else None, #char
                                "fiat": row["fiat"] if row["fiat"] else None, #Boolean
                                "instrument": row["instrument"] if row["instrument"] else None, #char
                                # "reversed_at": row["reversed_at"] if row["reversed_at"] else None, #blank(datetime)
                                "size": row["size"] if row["size"] else None, #(Float)
                                "status": row["status"] if row["status"] else None, #(Selection)
                                "active": True,
                                # "country_of_residence": row["country_of_residence"] if row["country_of_residence"] else None, #(Char)
                                # "time": row["time"] if row["time"] else None, #(blank : datetime)
                                "pmx_id": row["pmx_id"] if row["pmx_id"] else None, #(Required: char)
                                "client_pmx_id": row["client_pmx_id"] if row["client_pmx_id"] else None, #(Required: char)
                                "transaction_hash": row["transaction_hash"] if row["transaction_hash"] else None, #(Required: char)
                            }
                        print(withdrawal_vals)
                        withdrawal_data.append(withdrawal_vals)
                        if withdrawal_data:
                            columns = withdrawal_data[0].keys()
                            values = [tuple(item.get(column) for column in columns)for item in withdrawal_data]
                            insert_query = sql.SQL("INSERT INTO withdrawal ({}) VALUES ({})").format(
                                sql.SQL(", ").join(map(sql.Identifier, columns)),
                                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
                            )
                            withdrawal_data = []
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
