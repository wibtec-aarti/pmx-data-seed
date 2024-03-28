from faker import Faker
import psycopg2
import logging
import csv
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

# Initialize Faker
fake = Faker()

# Path to the CSV file for logging errors
LOG_FILE = "/home/odoo/Documents/wibtec/PP-1231/fs_client/fs_client_update_log_file.csv"
LOG_FILE_HEADER = ["file_name", "chunk_range", "record_id", "error"]


# Function to write error records to CSV
def write_error_to_csv(file_name, chunk_range, record_id, error_message):
    with open(LOG_FILE, mode="a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=LOG_FILE_HEADER)
        file_exists = os.path.exists(LOG_FILE)
        if not file_exists:
            writer.writeheader()
        writer.writerow(
            {
                "file_name": file_name,
                "chunk_range": chunk_range,
                "record_id": record_id,
                "error": error_message,
            }
        )


# Database connection parameter
dbname = "v16_PMX_07_03_2024"

# Set to store generated values for uniqueness
generated_values = set()

conn = psycopg2.connect(dbname=dbname)
logging.info("\nConnection established successfully!\n")
cur = conn.cursor()

# Black full legal name
cur.execute("UPDATE fs_client SET full_legal_name = ''")
conn.commit()

cur.execute(
    "SELECT id, first_name, middle_name, last_name, full_legal_name, street_1, street_2, address, phone_number, date_of_birth, tax_id, id_number FROM fs_client WHERE full_legal_name IS NOT NUll"
)

# Fetch records in batches
chunk_size = 10  # Adjust this according to your requirements
while True:
    chunk = cur.fetchmany(chunk_size)
    print("len(chunk)", len(chunk))
    if not chunk:
        break
    for record in chunk:
        client_id, first_name, middle_name, last_name, full_legal_name, street_1, street_2, address, phone_number, date_of_birth, tax_id, id_number = record
        new_first_name = fake.first_name()
        while new_first_name in generated_values:
            new_first_name = fake.first_name()
        generated_values.add(new_first_name)

        new_middle_name = fake.first_name()
        while new_middle_name in generated_values:
            new_middle_name = fake.first_name()
        generated_values.add(new_middle_name)

        new_last_name = fake.last_name()
        while new_last_name in generated_values:
            new_last_name = fake.last_name()
        generated_values.add(new_last_name)

        new_full_legal_name = f"{new_first_name} {new_middle_name} {new_last_name}"

        new_street_1 = fake.street_address()
        while new_street_1 in generated_values:
            new_street_1 = fake.street_address()
        generated_values.add(new_street_1)

        new_street_2 = fake.secondary_address()
        while new_street_2 in generated_values:
            new_street_2 = fake.secondary_address()
        generated_values.add(new_street_2)

        new_address = fake.address()
        while new_address in generated_values:
            new_address = fake.address()
        generated_values.add(new_address)

        new_phone_number = fake.phone_number()
        while new_phone_number in generated_values:
            new_phone_number = fake.phone_number()
        generated_values.add(new_phone_number)

        new_date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime(
            "%Y-%m-%d"
        )

        new_tax_id = fake.random_number(digits=9)

        new_id_number = fake.random_number(digits=9)
        while new_id_number in generated_values:
            new_id_number = fake.random_number(digits=9)
        generated_values.add(new_id_number)

        try:
            # Update the fields in the database
            cur.execute(
                "UPDATE fs_client SET first_name = %s, middle_name = %s, last_name = %s, full_legal_name = %s, street_1 = %s, street_2 = %s, address = %s, phone_number = %s, date_of_birth = %s, tax_id = %s, id_number = %s WHERE id = %s",
                (
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
                    client_id,
                ),
            )
            logging.info(f"Updated fields for client ID {client_id}")
        except Exception as e:
            # Log errors to CSV
            write_error_to_csv(LOG_FILE, "Chunk Range", client_id, str(e))
            logging.error(f"Error updating client ID {client_id}: {str(e)}")

    # Commit changes for each chunk
    conn.commit()

# Close the cursor and database connection
cur.close()
conn.close()

logging.info("Update complete. Database connection closed.")
