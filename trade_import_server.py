import psycopg2
import csv
import time
from psycopg2 import sql
import logging
from datetime import datetime, date

logging.basicConfig(level=logging.INFO)

# Connection To Server
dbname = "Master"
user = "odoo"
password = "AVNS_0byMv122u7uygOUkk4A"
host = "private-test-pmx-coresystems-do-user-2412463-0.c.db.ondigitalocean.com"
port = "25060"

# Path to the CSV file
csv_file = "/tmp/trades/file_chunks/chunk_1.csv"

# Record the start time
start_time = time.time()

# Establish connection to the database
try:
	conn = psycopg2.connect(
		dbname=dbname, user=user, password=password, host=host, port=port
	)
	logging.info("\nConnection established successfully!\n")
	
	# Create a cursor
	cur = conn.cursor()
	
	# Read data from CSV and insert into the database
	with open(csv_file, "r", encoding="latin1") as file:
		reader = list(csv.DictReader(file))
		logging.info(f"Size of csv file {len(reader)} , \n\n")
		num = 0

		batch_size = 1000

		for index in range(0, len(reader), batch_size):
			batch = reader[index : index + batch_size]

			for row in batch:
				num += 1
				logging.info(f"Row Number {num} & Trade record ID {row['id']}")

				cur.execute(f"SELECT * FROM trade WHERE id = {row['id']}")
				existing_trade_record = cur.fetchone()
				# logging.info(f"\nExisting Trade Record {existing_trade_record}")
				
				if not existing_trade_record:
					trade_pmx_vals = {
						"id": row["id"] if row["id"] else None,
						"create_date": row["create_date"] if row["create_date"] else None,
						"write_date": row["write_date"] if row["write_date"] else None,
						"account_id": row["x_accountid"] if row["x_accountid"] else None,
						"commission": row["x_commission"] if row["x_commission"] else 0.0,
						"commission_euro": row["x_commissioneur"]
						if row["x_commissioneur"]
						else 0.0,
						"created_at": row["x_datetime"] if row["x_datetime"] else None,
						"fee": row["x_fee"] if row["x_fee"] else None,
						"fee_rate": row["x_feerate"] if row["x_feerate"] else None,
						"source_currency": row["x_instrument"]
						if row["x_instrument"]
						else None,
						"price": row["x_price"] if row["x_price"] else None,
						"matched_quantity": row["x_quantity"]
						if row["x_quantity"]
						else None,
						"size": row["x_size"] if row["x_size"] else None,
						"time": row["x_time"]
						if row["x_time"]
						else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
						"trade_type": row["x_type"] if row["x_type"] else None,
						"amount_currency": row["x_amount_currency"]
						if row["x_amount_currency"]
						else None,
						"country_of_residence": row["x_studio_country_of_residence"]
						if row["x_studio_country_of_residence"]
						else None,
						"active": row["x_studio_active"]
						if row["x_studio_active"]
						else None,
						"fee_amount": row["x_feeamount"] if row["x_feeamount"] else None,
						"fee_amount_euro": row["x_feeamounteuro"]
						if row["x_feeamounteuro"]
						else None,
						"exchange_rate": row["x_instrument_rate"]
						if row["x_instrument_rate"]
						else None,
						"username": row["x_name"] if row["x_name"] else None,
						"pmx_id": row["x_name"] if row["x_name"] else None,
					}
					if row["x_client"]:
						cur.execute(
							"SELECT id, company_id, pmx_id FROM fs_client WHERE old_fs_client_id = %s",
							(row["x_client"],),
						)

						fs_client_record = cur.fetchone()
						if fs_client_record:
							trade_pmx_vals.update(
								{
									"fs_client_id": fs_client_record[0],
									"company_id": fs_client_record[1],
									"client_pmx_id": fs_client_record[2],
								}
							)

					#Random string   
					random_string = "".join(
						random.choices(string.ascii_lowercase + string.digits, k=8)
					)
					domain = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
					sub_domain = "".join(random.choices(string.ascii_lowercase, k=2))
					client_pmx_id = f"{'user'}|{random_string}@{domain}.{sub_domain}"
					trade_pmx_vals["client_pmx_id"] = client_pmx_id
					trade_data.append(trade_pmx_vals)
			
			columns = trade_data[0].keys()
			values = [tuple(item.get(column) for column in columns) for item in trade_data]
			insert_query = sql.SQL("INSERT INTO trade ({}) VALUES ({})").format(
				sql.SQL(', ').join(map(sql.Identifier, columns)),
				sql.SQL(', ').join(sql.Placeholder() * len(columns))
			)
			cur.executemany(insert_query, values)
			trade_data = []

except psycopg2.Error as e:
	logging.error(f"Error: Unable to import data\n{e}")
finally:
	if conn is not None:
		conn.commit()
		cur.close()
		conn.close()
		logging.info("\n\nConnection closed.")

# Calculate End time
end_time = time.time()
execution_duration = end_time - start_time
execution_duration_minutes = execution_duration / 60
print(f"\nTime taken: {execution_duration_minutes:.2f} minutes")