import psycopg2
import csv
import time
from psycopg2 import sql
import logging
from datetime import datetime, date
logging.basicConfig(level=logging.INFO)

# Connection To Server
# dbname = "Master"
# user = "odoo"
# password = "AVNS_0byMv122u7uygOUkk4A"
# host = "private-test-pmx-coresystems-do-user-2412463-0.c.db.ondigitalocean.com"
# port = "25060"

dbname = "perpetual_16_new"

# Path to the CSV file
csv_file = "/home/odoo/Documents/Aiendry-workspace/data_seeding_ftx_to_pmx/file_chunks/chunk_1.csv"

# Record the start time
start_time = time.time()
batch_size = 2000

# Establish connection to the database
try:
    conn = psycopg2.connect(dbname=dbname)
    
    logging.info("\nConnection established successfully!\n")

    # Create a cursor
    cur = conn.cursor()

    # Read data from CSV and insert into the database
    with open(csv_file, "r", encoding="latin1") as file:
        reader = csv.DictReader(file)
        num = 0  

        print("\nReader: ",reader)

        created_records = list()

        for row in reader:
            print("Row: ",row,'\n')
        
            if not row['id'].isdigit():
                continue  # Skip records with invalid 'id'

            num += 1
            print(f" Row Number {num} & Trade record ID {row['id']}" )

            trade_pmx_vals = {
                "id":row["id"],
                "create_uid":row["create_uid"] if row["create_uid"] else None ,
                "create_date":row["create_date"] if row["create_date"] else None,
                "write_uid":row["write_uid"] if row["write_uid"] else None,
                "write_date":row["write_date"] if row["write_date"] else None,
                "username":row["x_name"] if row["x_name"] else None,
                "pmx_id":row["x_FTXID"] if row["x_FTXID"] else None,
                "account_id":row["x_accountid"] if row["x_accountid"] else None,
                "commission":row["x_commission"] if row["x_commission"] else 0.0,
                "commission_euro":row["x_commissioneur"] if row["x_commissioneur"] else 0.0,
                "created_at":row["x_datetime"] if row["x_datetime"] else None,
                "fee":row["x_fee"] if row["x_fee"] else None,
                "fee_rate":row["x_feerate"] if row["x_feerate"] else None,
                "price":row["x_price"] if row["x_price"] else None,
                "matched_quantity":row["x_quantity"] if row["x_quantity"] else None,
                "size":row["x_size"] if row["x_size"] else None,
                "time":row["x_time"] if row["x_time"] else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "order_matching_engine_id":row["x_tradeid"] if row["x_tradeid"] else 0,
                "matching_engine_timestamp":row["x_tradetime"] if row["x_tradetime"] else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "trade_type":row["x_type"] if row["x_type"] else None,
                # "fs_client_id":row["x_client"] if row["x_client"] else None ,
                "currency_id":row["x_currency_id"] if row["x_currency_id"] else None,
                "country_of_residence":row["x_studio_country_of_residence"] if row["x_studio_country_of_residence"] else None,
                "active":row["x_studio_active"] if row["x_studio_active"] else None,
                # "company_id":row["x_company_id"] if row["x_company_id"] else None,
                "company_id" : 2, 
                "fee_amount":row["x_feeamount"] if row["x_feeamount"] else None,
                "fee_amount_euro":row["x_feeamounteuro"] if row["x_feeamounteuro"] else None,
                "client_pmx_id" : 1,
                }

            trade_columns = str(tuple(trade_pmx_vals.keys())).replace("'",'"')
            print("Trade columns: ",trade_columns,'\n')

            trade_values = str(tuple(trade_pmx_vals.values())).replace("None","Null")
            print("Trade values: ",trade_values,'\n')

            insert_sql = sql.SQL(f"INSERT INTO trade {trade_columns} VALUES {trade_values} RETURNING id")

            cur.execute(insert_sql)

            cur.commit()

            last_created_record_id = cur.fetchone()[0]

            print("last_created_record_id: ",last_created_record_id,'\n')

            created_records.append(last_created_record_id)

        print("Created Records: ",created_records,'\n')

except psycopg2.Error as e:
    print(f"Error: Unable to import data\n{e}")


finally:
    # Close the cursor and connection when done
    if conn is not None:
        conn.commit()
        cur.close()
        conn.close()
        print("\n\n\n Connection closed.")
