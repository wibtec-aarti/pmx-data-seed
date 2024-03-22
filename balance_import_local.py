import csv
import logging
import os
import random
import string
import time

import psycopg2
from psycopg2 import sql

logging.basicConfig(level=logging.INFO)

dbname = "v16_PMX_07_03_2024"

# Path to the CSV file
DIR_PATH = "/home/odoo/Documents/wibtec/PP-1231/balances/file_chunks"
LOG_FILE = "/home/odoo/Documents/wibtec/PP-1231/balances/balances_log_file.csv"
LOG_FILE_HEADER = ["file_name", "counter", "error"]
csv_files_name_list = os.listdir(DIR_PATH)
csv_files_name_list.sort()

# Establish connection to the database
try:
    conn = psycopg2.connect(dbname=dbname)
    logging.info("\nConnection established successfully!\n")
    cur = conn.cursor()

    cur.execute("SELECT old_fs_client_id, id, company_id, pmx_id FROM fs_client")
    fs_client_ref = {
        "%s" % old_fs_client_id: [new_id, company_id, pmx_id]
        for old_fs_client_id, new_id, company_id, pmx_id in cur.fetchall()
    }
    
    for file_name in csv_files_name_list:
        file_dir_path = "%s/%s" % (DIR_PATH, file_name)
        with open(LOG_FILE, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()

            # Record the start time
            start_time = time.time()

            # Read data from CSV and insert into the database
            with open(file_dir_path, "r", encoding="latin1") as file:
                reader = list(csv.DictReader(file))
                trade_data = []
                trade_pmx_vals = {}
                batch_size = 10
                num = 0
                counter = 0
                for index in range(0, len(reader), batch_size):
                    batch = reader[index: index + batch_size]
                    counter = index
                    for row in batch:
                        num += 1
                        logging.info(f"Row Number {num} & balance record ID {row['id']}")
                        cur.execute(f"SELECT * FROM balance WHERE id = {row['id']}")
                        existing_balance_record = cur.fetchone()
                        if not existing_balance_record:
                            balance_pmx_vals = {
                                "id": row["id"],
                                "create_date": row.get("create_date"),
                                "write_date": row["write_date"] if row["write_date"] else None,
                                "pmx_id": row["x_FTXID"] if row["x_FTXID"] else None,
                                "account_id": row["x_accountid"] if row["x_accountid"] else None,
                                "usd_value": row["x_amount"] if row["x_amount"] else None,
                                "amount_currency": row["x_amount_currency"] if row["x_amount_currency"] else None,
                                "balance_date": row["x_balancedate"] if row["x_balancedate"] else None,
                                "currency_id": row["x_currency_id"] if row["x_currency_id"] else None,
                                "instrument": row["x_instrument"] if row["x_instrument"] else None,
                                "active": row["x_studio_active"] if row["x_studio_active"] else None,
                                "country_of_residence": row["x_studio_country_of_residence"] if row[
                                    "x_studio_country_of_residence"] else None,
                                "type": row["x_type"] if row["x_type"] else None,
                            }
                            if row["x_client"] in fs_client_ref.keys():
                                balance_pmx_vals.update(
                                    {
                                        "fs_client_id": fs_client_ref[row["x_client"]][0],
                                        "company_id": fs_client_ref[row["x_client"]][1],
                                        "client_pmx_id": fs_client_ref[row["x_client"]][2],
                                    }
                                )
                            random_string = "".join(
                                random.choices(string.ascii_lowercase + string.digits, k=8)
                            )
                            domain = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
                            sub_domain = "".join(random.choices(string.ascii_lowercase, k=2))
                            client_pmx_id = f"{'user'}|{random_string}@{domain}.{sub_domain}"
                            balance_pmx_vals["client_pmx_id"] = client_pmx_id
                            balance_data.append(balance_pmx_vals)

                            columns = balance_data[0].keys()
                            values = [tuple(item.get(column) for column in columns) for item in balance_data]
                            insert_query = sql.SQL("INSERT INTO balance ({}) VALUES ({})").format(
                                sql.SQL(', ').join(map(sql.Identifier, columns)),
                                sql.SQL(', ').join(sql.Placeholder() * len(columns))
                            )
                            balance_data = []
                            try:
                                cur.executemany(insert_query, values)
                                conn.commit()
                            except psycopg2.Error as e:
                                mydict = [{'file_name': csv_file, 'counter': num, 'error': e}]
                                writer.writerows(mydict)
                                conn.commit()

                    mydict = [{'file_name': csv_file, 'counter': batch_size, 'error': 'N/A'}]
                    writer.writerows(mydict)
                    conn.commit()
            os.remove(csv_file)
except psycopg2.Error as e:
    logging.error(f"Error: Unable to import data\n{e}")
finally:
    if conn is not None:
        conn.commit()
        cur.close()
        conn.close()
        logging.info("\n\nConnection closed.")