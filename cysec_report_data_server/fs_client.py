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
LOG_FILE = "/opt/odoo/2086_seed_data/fs_client/fs_client_log.csv"
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
DIR_PATH = "/opt/odoo/2086_seed_data/fs_client/file_chunks/"
csv_files_name_list = os.listdir(DIR_PATH)
csv_files_name_list.sort()

try:
    conn = psycopg2.connect(dbname=dbname)
    logging.info("\nConnection established successfully!\n")
    cur = conn.cursor()

    #Search FS Client
    # cur.execute("SELECT old_fs_client_id, id, company_id, pmx_id FROM fs_client")
    # fs_client_ref = {
    #     "%s" % old_fs_client_id: [new_id, company_id, pmx_id]
    #     for old_fs_client_id, new_id, company_id, pmx_id in cur.fetchall()
    # }

    for file_name in csv_files_name_list:
        file_dir_path = "%s/%s" % (DIR_PATH, file_name)
        logging.info(f"Processing file: {file_name}")
        
        # Record the start time for file
        start_time = time.time()

        # Read data from CSV and insert into the database
        with open(file_dir_path, "r", encoding="utf-8") as file:
            reader = list(csv.DictReader(file))
            fs_client_data = []
            fs_client_vals = {}
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
                        logging.info(f"Processing file {file_name} : Row Number {num} : FS Client Record ID {current_row_id}")
                        cur.execute(f"SELECT * FROM fs_client WHERE id = {current_row_id}")
                        existing_fs_client_record = cur.fetchone()

                        if not existing_fs_client_record:
                            fs_client_vals = {
                                "active": True,
                                "id": row["id"],
                                "first_name": row["first_name"],
                                "middle_name": row["middle_name"],
                                "last_name": row["last_name"],
                                "full_legal_name": f"{row['first_name']} {row['middle_name']} {row['last_name']}".strip(),
                                "country_of_residence": row["country_of_residence"],
                                "pmx_id": row["pmx_id"],
                                "status": row["status"],
                                "client_categorization": row["client_categorization"],
                                "sector_of_client": row["sector_of_client"] if row["sector_of_client"] else None,
                            }
                            # if row["x_client"] in fs_client_ref.keys():
                            #     balance_pmx_vals.update(
                            #         {
                            #             "fs_client_id": fs_client_ref[row["x_client"]][0],
                            #             "old_fs_client_id": fs_client_ref[row["x_client"]][0],
                            #             "company_id": fs_client_ref[row["x_client"]][1],
                            #             "client_pmx_id": fs_client_ref[row["x_client"]][2]
                            #         }
                            #     )
                        fs_client_data.append(fs_client_vals)
                        if fs_client_data:
                            columns = fs_client_data[0].keys()
                            values = [tuple(item.get(column) for column in columns)for item in fs_client_data]
                            insert_query = sql.SQL("INSERT INTO fs_client ({}) VALUES ({})").format(
                                sql.SQL(", ").join(map(sql.Identifier, columns)),
                                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
                            )
                            fs_client_data = []
                            cur.executemany(insert_query, values)
                            conn.commit()
                    except psycopg2.Error as error:
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
