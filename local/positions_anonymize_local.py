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
LOG_FILE = ("/home/odoo/Documents/wibtec/PP-1231/positions/positions_update_log_file.csv")
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

# Set to keep track of generated numbers
generated_numbers = set()

# Function to generate a random unique number
def generate_random_unique_number():
    while True:
        random_number = fake.random_number(digits=9) # Generate a random 9-digit number
        if random_number not in generated_numbers:  # Check if the number is unique
            generated_numbers.add(random_number)  # Add the number to the set
            return random_number

try:
    # Database connection parameter
    dbname = "v16_PMX_07_03_2024"

    conn = psycopg2.connect(dbname=dbname)
    logging.info("\nConnection established successfully!\n")
    cur = conn.cursor()

    #Search fs client record
    cur.execute("SELECT id FROM position WHERE pmx_id = '' ORDER BY id LIMIT 2000")
    position_ids = [row[0] for row in cur.fetchall()]

    # Define batch size
    position_list = []
    batch_size = 1000
    num = 0
    counter = 0
    for index in range(0, len(position_ids), batch_size):
        batch = position_ids[index : index + batch_size]
        counter = index
        for row in batch:
            try:
                num+=1
                logging.info(f"Processing : Row Number {num} : Position Record ID {row}")
                random_number = generate_random_unique_number()
                new_pmx_id = f"position{random_number}"
                new_account_id = random_number

                position_list.append((
                    new_pmx_id,
                    new_account_id,
                    row,
                ))
                query = """
                UPDATE position
                SET
                    pmx_id = %s,
                    account_id = %s
                WHERE
                    id = %s
                """
                cur.executemany(query, position_list)
                conn.commit()
                position_list = []
            except psycopg2.Error as error:
                conn.rollback()
                print(error)
                error_message = f"Error: Unable to import data row {row}. Error: {error}"
                write_error_to_csv(num, f"Row Number: {row}",error_message)
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