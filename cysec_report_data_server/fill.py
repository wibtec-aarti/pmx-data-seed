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
LOG_FILE = "/opt/odoo/2086_seed_data/fill/fill_log.csv"
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
DIR_PATH = "/opt/odoo/2086_seed_data/fill/file_chunks/"
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
            fill_data = []
            fill_vals = {}
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
                        logging.info(f"Processing file {file_name} : Row Number {num} : fill Record ID {current_row_id}")
                        cur.execute(f"SELECT * FROM fill WHERE id = {current_row_id}")
                        existing_fill_record = cur.fetchone()

                        if not existing_fill_record:
                            fill_vals = {
                                "active": True,
                                "id": row["id"],
                                "pmx_id": row["pmx_id"] if row["pmx_id"] else None, # (not model)
                                "client_pmx_id": row["client_pmx_id"] if row["client_pmx_id"] else None, # (not model)
                                # "trade_id": row["trade_idtrade_id"] if row["trade_id"] else None, # (not model)
                                # "taker_order_id": row["taker_order_id"] if row["taker_order_id"] else None, # (not model)
                                # "taker_side": row["taker_side"] if row["taker_side"] else None, # (not model)
                                # "taker_fee": row["taker_fee"] if row["taker_fee"] else None, # (not model)
                                # "maker_order_id": row["maker_order_id"] if row["maker_order_id"] else None, # (not model)
                                # "maker_side": row["maker_side"] if row["maker_side"] else None, # (not model)
                                # "maker_currency_fee": row["maker_currency_fee"] if row["maker_currency_fee"] else None, # (not model)
                                # "maker_fee": row["maker_fee"] if row["maker_fee"] else None, # (not model)
                                # "created_at": row["created_at"] if row["created_at"] else None, # (not model)
                                # "fee_instrument": row["fee_instrument"] if row["fee_instrument"] else None, # (not model)
                                # "fee_rate": row["fee_rate"] if row["fee_rate"] else None, # (not model)
                                # "fee_amount_euro": row["fee_amount_euro"] if row["fee_amount_euro"] else None, # (not model)
                                # "source_currency": row["source_currency"] if row["source_currency"] else None, # (not model)
                                "price": row["price"] if row["price"] else None, # (Float)
                                # "matched_quantity": row["matched_quantity"] if row["matched_quantity"] else None, # (not model)
                                "size": row["size"] if row["size"] else None, # (Float)
                                # "time": row["time"] if row["time"] else None, # (Not model)
                                # "matching_engine_timestamp": row["matching_engine_timestamp"] if row["matching_engine_timestamp"] else None, # (Not model)
                                "side": row["side"] if row["side"] else None, # (Char)
                                # "trade_type": row["trade_type"] if row["trade_type"] else None, # (NOt in model)
                                # "eur_value": row["eur_value"] if row["eur_value"] else None, # (NOt in model)
                            }
                        print(fill_vals)
                        fill_data.append(fill_vals)
                        print("fill_data>>>", fill_data)
                        if fill_data:
                            columns = fill_data[0].keys()
                            values = [tuple(item.get(column) for column in columns)for item in fill_data]
                            insert_query = sql.SQL("INSERT INTO fill ({}) VALUES ({})").format(
                                sql.SQL(", ").join(map(sql.Identifier, columns)),
                                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
                            )
                            fill_data = []
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
