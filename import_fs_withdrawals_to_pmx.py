import psycopg2
import csv
import time
from psycopg2 import sql
import logging
from datetime import datetime, date
import pandas as pd

logging.basicConfig(level=logging.INFO)

# Connection To Server
# dbname = "Master"
# user = "odoo"
# password = "AVNS_0byMv122u7uygOUkk4A"
# host = "private-test-pmx-coresystems-do-user-2412463-0.c.db.ondigitalocean.com"
# port = "25060"

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

        for row in reader:
            print("\n x_client: ", row['x_client'])
            num += 1
            print(f"\n\n Row Number {num} & withdrawal record ID {row['id']}")
            print("\n\n row >>", row)
            print("\n\n\n Record x client : ", row["x_client"], type(row["x_client"]))

            cur.execute(f"SELECT * FROM withdrawal WHERE id = {row['id']}")
            existing_record = cur.fetchone()
            print(f"\n\n\n Existing Record {existing_record}")

            if not existing_record:
                print("Record Id does not exist in the DB: ", row["id"])

                if not row['x_client']:
                    print("\n x Client id is not found")
                    continue
                cur.execute(f"SELECT * FROM fs_client WHERE old_fs_client_id = {row['x_client']}")
                fs_client_record = cur.fetchone()
                print(f"\n\n\n fs_client_record  {fs_client_record}")


                if fs_client_record:
                    # Get the column names from the cursor description
                    column_names = [desc[0] for desc in cur.description]
                    print(f"\n\n\n column_names  {column_names}")

                    # # Create a dictionary to store column names and corresponding values
                    fs_client_record_dict = dict(zip(column_names, fs_client_record))
                    # print(f"\n\n\n fs_client_record_dict  {fs_client_record_dict}")

                    fs_client_id = fs_client_record_dict['id']
                    print(f"\n\n\n fs_client_id  {fs_client_id}")
                    pmx_id = fs_client_record_dict['pmx_id']
                    company_id = fs_client_record_dict['company_id']

                    withdrawals_pmx_vals = {
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
                        "status": row["x_status"] if row["x_status"] else None,
                        "active": row["x_studio_active"] if row["x_studio_active"] else None,
                        "country_of_residence": row["x_studio_country_of_residence"] if row["x_studio_country_of_residence"] else None,
                        "time": row["x_time"] if row["x_time"] else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "fs_client_id": fs_client_id,
                        "company_id": company_id,
                        "pmx_id": row["x_name"] if row["x_name"] else None,
                        "client_pmx_id": pmx_id
                    }
                    # print("Trade PMX Vals: ", trade_pmx_vals, "\n")

                    withdrawal_columns = str(tuple(withdrawals_pmx_vals.keys())).replace("'", '"')
                    print("withdrawal columns: ", withdrawal_columns, "\n")

                    withdrawal_values = str(tuple(withdrawals_pmx_vals.values())).replace("None", "Null")
                    print("withdrawal values: ", withdrawal_values, "\n")

                    insert_sql = sql.SQL(
                        f"INSERT INTO withdrawal {withdrawal_columns} VALUES {withdrawal_values} RETURNING id")
                    print("insert_sql: ", insert_sql, "\n")
                    cur.execute(insert_sql)

            if num == 5:
                break

except psycopg2.Error as e:
    print(f"Error: Unable to import data\n{e}")

finally:
    # Close the cursor and connection when done
    if conn is not None:
        conn.commit()
        cur.close()
        conn.close()
        print("\n\n\n Connection closed.")
