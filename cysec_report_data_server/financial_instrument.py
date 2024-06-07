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
LOG_FILE = "/opt/odoo/2086_seed_data/financial_instrument/financial_instrument_log.csv"
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
DIR_PATH = "/opt/odoo/2086_seed_data/financial_instrument/file_chunks/"
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
            financial_instrument_data = []
            financial_instrument_vals = {}
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
                        logging.info(f"Processing file {file_name} : Row Number {num} : financial_instrument Record ID {current_row_id}")
                        cur.execute(f"SELECT * FROM financial_instrument WHERE id = {current_row_id}")
                        existing_financial_instrument_record = cur.fetchone()

                        if not existing_financial_instrument_record:
                            financial_instrument_vals = {
                                
                                "id": row["id"],
                                "instrument": row["instrument"] if row["instrument"] else None, # (char)
                                "category": row["category"] if row["category"] else None, # (selection)
                                "contract_type": row["contract_type"] if row["contract_type"] else None, # (selection)
                                "is_otc": row["is_otc"] if row["is_otc"] else None, 
                                "is_repo": row["is_repo"] if row["is_repo"] else None, 
                                "is_sustainable_product": row["is_sustainable_product"] if row["is_sustainable_product"] else None, 
                                "is_fiat": row["is_fiat"] if row["is_fiat"] else None, 
                                "investment_services_activities": row["investment_services_activities"] if row["investment_services_activities"] else None, 
                                "ancillary_services": row["ancillary_services"] if row["ancillary_services"] else None, 
                                "underlying_ticker": row["underlying_ticker"] if row["underlying_ticker"] else None, 
                                "underlying_name": row["underlying_name"] if row["underlying_name"] else None, 
                               
                            }
                        print(financial_instrument_vals)
                        financial_instrument_data.append(financial_instrument_vals)
                        print("financial_instrument_data>>>", financial_instrument_data)
                        if financial_instrument_data:
                            columns = financial_instrument_data[0].keys()
                            values = [tuple(item.get(column) for column in columns)for item in financial_instrument_data]
                            insert_query = sql.SQL("INSERT INTO financial_instrument ({}) VALUES ({})").format(
                                sql.SQL(", ").join(map(sql.Identifier, columns)),
                                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
                            )
                            financial_instrument_data = []
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
