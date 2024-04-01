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
LOG_FILE = "/home/odoo/Documents/wibtec/PP-1231/positions/positions_log_file.csv"
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
DIR_PATH = "/home/odoo/Documents/wibtec/PP-1231/positions/file_chunks"
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
			positions_data = []
			positions_pmx_vals = {}
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
						logging.info(f"Processing file {file_name} : Row Number {num} : position Record ID {current_row_id}")
						cur.execute(f"SELECT * FROM position WHERE id = {current_row_id}")
						existing_position_record = cur.fetchone()

						if not existing_position_record:
							position_pmx_vals = {
								"id": row["id"],
								"create_date": row["create_date"] if row["create_date"] else None,
								"write_date": row["write_date"] if row["write_date"] else None,
								"pmx_id": row["x_FTXID"] if row["x_FTXID"] else None,
								"account_id": row["x_accountid"] if row["x_accountid"] else None,
								"usd_value": row["x_amount"] if row["x_amount"] else None,
								"amount_currency": row["x_amount_currency"] if row["x_amount_currency"] else None,
								"currency_id": row["x_currency_id"] if row["x_currency_id"] else None,
								"eur_value": row["x_eurvalue"] if row["x_eurvalue"] else None,
								"instrument": row["x_instrument"] if row["x_instrument"] else None,
								"exchange_rate": row["x_instrument_rate"] if row["x_instrument_rate"] else None,
								"market": row["x_market"] if row["x_market"] else None,
								"size": row["x_position_size"] if row["x_position_size"] else None,
								"active": row["x_studio_active"] if row["x_studio_active"] else None,
								"country_of_residence": row["x_studio_country_of_residence"] if row["x_studio_country_of_residence"] else None,
								"type": row["x_type"] if row["x_type"] else None,
							}
							if row["x_client"] in fs_client_ref.keys():
								position_pmx_vals.update(
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
							position_pmx_vals["client_pmx_id"] = client_pmx_id
							positions_data.append(position_pmx_vals)

						if positions_data:
							columns = positions_data[0].keys()
							values = [tuple(item.get(column) for column in columns)for item in positions_data]
							insert_query = sql.SQL("INSERT INTO position ({}) VALUES ({})").format(
								sql.SQL(", ").join(map(sql.Identifier, columns)),
								sql.SQL(", ").join(sql.Placeholder() * len(columns)),
							)
							positions_data = []
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
