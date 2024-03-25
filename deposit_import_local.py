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
LOG_FILE = "/home/odoo/Documents/wibtec/PP-1231/deposit/deposit_log_file.csv"
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
dbname = "v16_PMX_07_03_2024"

# Define directory 
DIR_PATH = "/home/odoo/Documents/wibtec/PP-1231/deposit/file_chunks"
csv_files_name_list = os.listdir(DIR_PATH)
csv_files_name_list.sort()

try:
	conn = psycopg2.connect(dbname=dbname)
	logging.info("\nConnection established successfully!\n")
	cur = conn.cursor()

	#Search FS Client
	cur.execute("SELECT old_fs_client_id, id, company_id, pmx_id FROM fs_client")
	fs_client_ref = {
		"%s" % old_fs_client_id: [new_id, company_id, pmx_id]
		for old_fs_client_id, new_id, company_id, pmx_id in cur.fetchall()
	}

	for file_name in csv_files_name_list:
		file_dir_path = "%s/%s" % (DIR_PATH, file_name)
		logging.info(f"Processing file: {file_name}")
		
		# Record the start time for file
		start_time = time.time()

		# Read data from CSV and insert into the database
		with open(file_dir_path, "r", encoding="utf-8") as file:
			reader = list(csv.DictReader(file))
			deposit_data = []
			deposit_pmx_vals = {}
			batch_size = 1000
			num = 0
			counter = 0
			for index in range(0, len(reader), batch_size):
				batch = reader[index : index + batch_size]
				counter = index
				for row in batch:
					try:
						num += 1
						current_row_id = row.get('id')
						logging.info(f"Processing file {file_name} : Row Number {num} : Deposit Record ID {current_row_id}")
						cur.execute(f"SELECT * FROM deposit WHERE id = {current_row_id}")
						existing_deposit_record = cur.fetchone()

						if not existing_deposit_record:
							deposit_pmx_vals = {
								"id": row.get('id'),
								"create_date": row.get("create_date"),
								"write_date": row.get("write_date"),
								"pmx_id": row.get("x_FTXID"),
								"account_id": row.get("x_accountid"),
								"usd_value": row.get("x_amount"),
								"amount_currency": row.get("x_amount_currency"),
								"approved_deposit": row.get("x_approved_deposit"),
								"bank": row.get("x_bank"),
								"created_at": row.get("x_createdat"),
								"currency_id": row.get("x_currency_id"),
								"fee": row.get("x_fee"),
								"fiat": row.get("x_fiat"),
								"instrument": row.get("x_instrument"),
								"exchange_rate": row.get("x_instrument_rate"),
								"received_at": row.get("x_receivedat"),
								"reversed_at": row.get("x_reversedat"),
								"size": row.get("x_size"),
								"active": row.get("x_studio_active"),
								"country_of_residence": row.get("x_studio_country_of_residence"),
								"time": row.get("x_time"),
								"username": row.get("x_username")
							}
							if row["x_client"] in fs_client_ref.keys():
								deposit_pmx_vals.update(
									{
										"fs_client_id": fs_client_ref[row["x_client"]][0],
										"old_fs_client_id": fs_client_ref[row["x_client"]][0],
										"company_id": fs_client_ref[row["x_client"]][1],
										"client_pmx_id": fs_client_ref[row["x_client"]][2]
									}
								)
							#Generate Random String
							random_string = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
							domain = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
							sub_domain = "".join(random.choices(string.ascii_lowercase, k=2))
							client_pmx_id = (f"{'user'}|{random_string}@{domain}.{sub_domain}")
							deposit_pmx_vals["client_pmx_id"] = client_pmx_id
							deposit_data.append(deposit_pmx_vals)

						if deposit_data:
							columns = deposit_data[0].keys()
							values = [tuple(item.get(column) for column in columns)for item in deposit_data]
							insert_query = sql.SQL("INSERT INTO deposit ({}) VALUES ({})").format(
								sql.SQL(", ").join(map(sql.Identifier, columns)),
								sql.SQL(", ").join(sql.Placeholder() * len(columns)),
							)
							deposit_data = []
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
