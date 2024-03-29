from faker import Faker
import psycopg2
import logging
import time
import csv
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

# Initialize Faker
fake = Faker()

# Record the script start time for file
script_start_time = time.time()

# Path to the CSV file for logging errors
LOG_FILE = ("/home/odoo/Documents/wibtec/PP-1231/fs_client/fs_client_update_log_file.csv")
LOG_FILE_HEADER = ["batch", "record_id", "error"]


# Function to write error records to CSV
def write_error_to_csv(batch, record_id, error_message):
    with open(LOG_FILE, mode="a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=LOG_FILE_HEADER)
        file_exists = os.path.exists(LOG_FILE)
        if not file_exists:
            writer.writeheader()
        writer.writerow(
            {
                "batch": batch,
                "record_id": record_id,
                "error": error_message,
            }
        )

try:
    # Database connection parameter
    dbname = "Master"
    user = "odoo"
    password = "AVNS_0byMv122u7uygOUkk4A"
    host = "private-test-pmx-coresystems-do-user-2412463-0.c.db.ondigitalocean.com"
    port = "25060"

    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    logging.info("\nConnection established successfully!\n")
    cur = conn.cursor()

    #Search fs client record
    cur.execute("SELECT id FROM fs_client WHERE full_legal_name = '' ORDER BY id")
    fs_client_ids = [row[0] for row in cur.fetchall()]

    # Define batch size
    fs_client_list = []
    batch_size = 1000
    num = 0
    counter = 0
    for index in range(0, len(fs_client_ids), batch_size):
        batch = fs_client_ids[index : index + batch_size]
        counter = index
        for row in batch:
            try:
                num+=1
                logging.info(f"Processing : Row Number {num} : FS Client Record ID {row}")
                new_first_name = fake.first_name()
                new_middle_name = fake.first_name()
                new_last_name = fake.last_name()
                new_full_legal_name = f"{new_first_name} {new_middle_name} {new_last_name}"
                new_street_1 = fake.street_address()
                new_street_2 = fake.secondary_address()
                new_address = fake.address()
                new_phone_number = fake.phone_number()
                new_date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d")
                new_tax_id = fake.random_number(digits=9)
                new_id_number = fake.random_number(digits=9)

                fs_client_list.append((
                    new_first_name,
                    new_middle_name,
                    new_last_name,
                    new_full_legal_name,
                    new_street_1,
                    new_street_2,
                    new_address,
                    new_phone_number,
                    new_date_of_birth,
                    new_tax_id,
                    new_id_number,
                    row,
                ))
                query = """
                UPDATE fs_client
                SET
                    first_name = %s,
                    middle_name = %s,
                    last_name = %s,
                    full_legal_name = %s,
                    street_1 = %s,
                    street_2 = %s, 
                    address = %s,
                    phone_number = %s,
                    date_of_birth = %s, 
                    tax_id = %s,
                    id_number = %s
                WHERE
                    id = %s
                """
                cur.executemany(query, fs_client_list)
                conn.commit()
                fs_client_list = []
            except psycopg2.Error as error:
                conn.rollback()
                error_message = f"Error: Unable to import data row {row}. Error: {error}"
                write_error_to_csv("N/A", f"Row Number: {row}",error_message)
                continue
        write_error_to_csv(f"{counter + 1}-{num}", 'N/A', 'N/A')
finally:
    if conn is not None:
        cur.close()
        conn.close()
        logging.info("Connection closed.")

# Calculate End time
script_end_time = time.time()
script_execution_duration = script_end_time - script_start_time
script_execution_duration_minutes = script_execution_duration / 60
logging.info(f"\n\n\n\n Script Time taken : {script_execution_duration_minutes} minutes")