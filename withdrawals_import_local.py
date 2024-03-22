import psycopg2
import csv
import time
from psycopg2 import sql
import logging
from datetime import datetime, date
import pandas as pd

logging.basicConfig(level=logging.INFO)

dbname = "pmx_seed_v16"

# Path to the CSV file
csv_file = ("/home/odoo/Documents/PP-1231/withdraws/withdrawals.csv")

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
        num = 0
        withdrawal_data = []

        for row in reader:
            num += 1
            logging.info(f"Row Number {num} & Withdrawal record ID {row['id']}")

            cur.execute(f"SELECT * FROM withdrawal WHERE id = {row['id']}")
            existing_withdrawal_record = cur.fetchone()
            logging.info(f"\nExisting Withdrawal Record {existing_withdrawal_record}")

            if not existing_withdrawal_record:
                withdrawal_pmx_vals = {
                    "id": row["id"],
                    "create_date": row["create_date"] if row["create_date"] else None,
                    "write_date": row["write_date"] if row["write_date"] else None,
                    "account_id": row["x_accountid"] if row["x_accountid"] else None,
                    "usd_value": row["x_amount"] if row["x_amount"] else None,
                    "amount_currency": row["x_amount_currency"] if row["x_amount_currency"] else None,
                    "approved_withdrawal": row["x_approved_withdrawal"] if row["x_approved_withdrawal"] else None,
                    "bank": row["x_bank"] if row["x_bank"] else None,
                    "created_at": row["x_createdat"] if row["x_createdat"] else None,
                    "currency_id": row["x_currency_id"] if row["x_currency_id"] else None,
                    "destination_srn": row["x_destination"] if row["x_destination"] else None,
                    "error": row["x_errorcode"] if row["x_errorcode"] else None,
                    "banking_details": row["x_extrabankinfo"] if row["x_extrabankinfo"] else None,
                    "fee": row["x_fee"] if row["x_fee"] else None,
                    "fiat": row["x_fiat"] if row["x_fiat"] else None,
                    "instrument": row["x_instrument"] if row["x_instrument"] else None,
                    "exchange_rate": row["x_instrument_rate"] if row["x_instrument_rate"] else None,
                    "received_at": row["x_receivedat"] if row["x_receivedat"] else None,
                    "size": row["x_size"] if row["x_size"] else None,
                    "active": row["x_studio_active"] if row["x_studio_active"] else None,
                    "country_of_residence": row["x_studio_country_of_residence"] if row[
                        "x_studio_country_of_residence"] else None,
                    "time": row["x_time"] if row["x_time"] else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "pmx_id": row["x_FTXID"] if row["x_FTXID"] else None,
                    "client_pmx_id": row.get("x_useremail") if row.get("x_useremail") else None
                }

                if row['x_client']:
                    cur.execute("SELECT id, company_id, pmx_id FROM fs_client WHERE old_fs_client_id = %s",
                                (row['x_client'],))

                    fs_client_record = cur.fetchone()
                    print("\n\n Value of the fs_client record is: ", fs_client_record)
                    if fs_client_record:
                        withdrawal_pmx_vals.update({
                            "fs_client_id": fs_client_record[0],
                            "company_id": fs_client_record[1],

                        })
                    else:
                        withdrawal_pmx_vals["client_pmx_id"] = row.get("x_useremail") if row.get(
                            "x_useremail") else None

                withdrawal_data.append(withdrawal_pmx_vals)
                print("\nwithdrawal PMX Vals: ", withdrawal_pmx_vals)

                columns = withdrawal_data[0].keys()
                values = [tuple(item.get(column) for column in columns) for item in withdrawal_data]

                insert_query = sql.SQL("INSERT INTO withdrawal ({}) VALUES ({})").format(
                    sql.SQL(', ').join(map(sql.Identifier, columns)),
                    sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )

                cur.executemany(insert_query, values)
                withdrawal_data = []

            if num == 5000:
                break

except psycopg2.Error as e:
    logging.error(f"Error: Unable to import data\n{e}")

finally:
    if conn is not None:
        conn.commit()
        cur.close()
        conn.close()
        logging.info("\n\nConnection closed.")