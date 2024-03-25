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
LOG_FILE = "/home/odoo/Documents/wibtec/PP-1231/trades/trades_log_file.csv"
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
DIR_PATH = "/home/odoo/Documents/wibtec/PP-1231/trades/file_chunks"
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
			trade_data = []
			trade_pmx_vals = {}
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
						logging.info(f"Processing file {file_name} : Row Number {num} : Trade Record ID {current_row_id}")
						cur.execute(f"SELECT * FROM trade WHERE id = {current_row_id}")
						existing_trade_record = cur.fetchone()

						if not existing_trade_record:
							trade_pmx_vals = {
								"id": row.get('id'),
								"create_date": row.get("create_date"),
								"write_date": row.get("write_date"),
								"account_id": row.get("x_accountid"),
								"commission": row["x_commission"] if row["x_commission"] else 0.0,
								"commission_euro": row["x_commissioneur"] if row["x_commissioneur"] else 0.0,
								"created_at": row.get("x_datetime"),
								"fee": row.get("x_fee"),
								"fee_rate": row.get("x_feerate"),
								"source_currency": row.get("x_instrument"),
								"price": row.get("x_price"),
								"matched_quantity": row.get("x_quantity"),
								"size": row.get("x_size"),
								"time": row["x_time"] if row["x_time"] else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
								"trade_type": row.get("x_type"),
								"amount_currency": row.get("x_amount_currency"),
								"country_of_residence": row.get("x_studio_country_of_residence"),
								"active": row.get("x_studio_active"),
								"fee_amount": row.get("x_feeamount"),
								"fee_amount_euro": row.get("x_feeamounteuro"),
								"exchange_rate": row.get("x_instrument_rate"),
								"username": row.get("x_name"),
								"pmx_id": row.get("x_name")
							}
							if row["x_client"] in fs_client_ref.keys():
								trade_pmx_vals.update(
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
							trade_pmx_vals["client_pmx_id"] = client_pmx_id
							trade_data.append(trade_pmx_vals)

						if trade_data:
							columns = trade_data[0].keys()
							values = [tuple(item.get(column) for column in columns)for item in trade_data]
							insert_query = sql.SQL("INSERT INTO trade ({}) VALUES ({})").format(
								sql.SQL(", ").join(map(sql.Identifier, columns)),
								sql.SQL(", ").join(sql.Placeholder() * len(columns)),
							)
							trade_data = []
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
		os.remove(file_dir_path)
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
