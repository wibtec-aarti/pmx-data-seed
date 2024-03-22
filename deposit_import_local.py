import psycopg2
import csv
import time
from psycopg2 import sql
import logging
from datetime import datetime, date
import pandas as pd
import random
import string

logging.basicConfig(level=logging.INFO)

dbname = "v16_PMX_07_03_2024"

# Path to the CSV file
csv_file = "/home/odoo/Documents/wibtec/PP-1231/deposit/file_chunks/chunk_1.csv"

# Record the start time
start_time = time.time()

# Establish connection to the database
try:
    conn = psycopg2.connect(dbname=dbname)
    logging.info("\nConnection established successfully!\n")

    # Create a cursor
    cur = conn.cursor()

    # Read data from CSV and insert into the database
    with open(csv_file, "r", encoding="latin1") as file:
        reader = list(csv.DictReader(file))
        logging.info(f"Size of reader: {len(reader)}")
        deposit_data = []
        num = 0
        batch_size = 100

        for index in range(0, len(reader), batch_size):
            batch = reader[index: index + batch_size]

            for row in batch:
                num += 1
                logging.info(f"Row Number {num} & deposit record ID {row['id']}")

                cur.execute(f"SELECT * FROM deposit WHERE id = {row['id']}")
                existing_deposit_record = cur.fetchone()
                # logging.info(f"\nExisting deposit Record {existing_deposit_record}")

                if not existing_deposit_record:
                    deposit_pmx_vals = {
                        "id": row["id"],
                        "create_date": row["create_date"] if row["create_date"] else None,
                        "write_date": row["write_date"] if row["write_date"] else None,
                        "pmx_id": row["x_FTXID"] if row["x_FTXID"] else None,
                        "account_id": row["x_accountid"] if row["x_accountid"] else None,
                        "usd_value": row["x_amount"] if row["x_amount"] else None,
                        "amount_currency": row["x_amount_currency"] if row["x_amount_currency"] else None,
                        "approved_deposit": row["x_approved_deposit"] if row["x_approved_deposit"] else None,
                        "bank": row["x_bank"] if row["x_bank"] else None,
                        "created_at": row["x_createdat"] if row["x_createdat"] else None,
                        "currency_id": row["x_currency_id"] if row["x_currency_id"] else None,
                        "fee": row["x_fee"] if row["x_fee"] else None,
                        "fiat": row["x_fiat"] if row["x_fiat"] else None,
                        "instrument": row["x_instrument"] if row["x_instrument"] else None,
                        "exchange_rate": row["x_instrument_rate"] if row["x_instrument_rate"] else None,
                        "received_at": row["x_receivedat"] if row["x_receivedat"] else None,
                        "reversed_at": row["x_reversedat"] if row["x_reversedat"] else None,
                        "size": row["x_size"] if row["x_size"] else None,
                        "active": row["x_studio_active"] if row["x_studio_active"] else None,
                        "country_of_residence": row["x_studio_country_of_residence"] if row[
                            "x_studio_country_of_residence"] else None,
                        "time": row["x_time"] if row["x_time"] else None,
                        "username": row["x_username"] if row["x_username"] else None
                    }
                    if row["x_client"]:
                        cur.execute(
                            "SELECT id, company_id, pmx_id FROM fs_client WHERE old_fs_client_id = %s",
                            (row["x_client"],),
                        )

                        fs_client_record = cur.fetchone()
                        if fs_client_record:
                            deposit_pmx_vals.update(
                                {
                                    "fs_client_id": fs_client_record[0],
                                    "company_id": fs_client_record[1],
                                    "client_pmx_id": fs_client_record[2],
                                }
                            )
                        random_string = "".join(
                            random.choices(string.ascii_lowercase + string.digits, k=8)
                        )
                    domain = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
                    sub_domain = "".join(random.choices(string.ascii_lowercase, k=2))
                    client_pmx_id = f"{'user'}|{random_string}@{domain}.{sub_domain}"
                    deposit_pmx_vals["client_pmx_id"] = client_pmx_id
                    deposit_data.append(deposit_pmx_vals)

                    columns = deposit_data[0].keys()
                    values = [tuple(item.get(column) for column in columns) for item in deposit_data]
                    insert_query = sql.SQL("INSERT INTO deposit ({}) VALUES ({})").format(
                        sql.SQL(', ').join(map(sql.Identifier, columns)),
                        sql.SQL(', ').join(sql.Placeholder() * len(columns))
                    )
                    cur.executemany(insert_query, values)
                    deposit_data = []
                    conn.commit()
except psycopg2.Error as e:
    logging.error(f"Error: Unable to import data\n{e}")
finally:
    if conn is not None:
        conn.commit()
        cur.close()
        conn.close()
        logging.info("\n\nConnection closed.")
