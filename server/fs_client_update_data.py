import pandas as pd
import time
import random
import string

# Record the start time
start_time = time.time()


update_file_path = "x_fs_client_update.csv"
output_file_path = "x_fs_client_output.csv"

row_count = 0
col_count = 0


chunk_size = 100000  # Adjust the chunk size based on your available memory

df = pd.DataFrame()
for chunk in pd.read_csv(
    update_file_path,
    encoding="utf-8",
    chunksize=chunk_size,
    low_memory=False,
    lineterminator="\n",
):
    df = pd.concat([df, chunk], ignore_index=True)
    row_count += len(chunk)
    col_count = max(col_count, len(chunk.columns))

    for index, row in chunk.iterrows():
        # Generate a random string of 8 characters
        random_string = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        domain = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
        sub_domain = "".join(random.choices(string.ascii_lowercase, k=2))
        new_email = f"{'user'}|{random_string}@{domain}.{sub_domain}"

        # Update the data in the DataFrame
        df.at[index, "pmx_id"] = new_email
        df.at[index, "email"] = f"{random_string}@{domain}.{sub_domain}"

df.to_csv(output_file_path, index=False, encoding="utf-8")

print(f"Total count of the rows in update CSV file: {row_count}")
print(f"Total count of the columns in update CSV file: {col_count}")

print("\n\n Output file ready !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# Record the end time
end_time = time.time()

# Calculate the execution duration
execution_duration = end_time - start_time

# Convert execution time to minutes
execution_duration_minutes = execution_duration / 60
print(f"\nTime taken: {execution_duration_minutes:.2f} minutes")
