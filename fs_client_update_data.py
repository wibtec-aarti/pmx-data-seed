import pandas as pd
import time
import random
import string

# Record the start time
start_time = time.time()


update_file_path = '/home/odoo/Documents/wibtec/PP-1231/fs_client/x_fs_client_update.csv'
output_file_path = '/home/odoo/Documents/wibtec/PP-1231/fs_client/x_fs_client_output.csv'

row_count = 0
col_count = 0


chunk_size = 100000  # Adjust the chunk size based on your available memory

df = pd.DataFrame()
for chunk in pd.read_csv(update_file_path, encoding='latin1', chunksize=chunk_size, low_memory=False,lineterminator='\n'):
	df = pd.concat([df, chunk], ignore_index=True)
	row_count += len(chunk)  
	col_count = max(col_count, len(chunk.columns)) 
	print("\n\n chunk >> ",  len(chunk))

	for index, row in chunk.iterrows():
		# Generate a random string of 8 characters
		random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
		domain = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
		sub_domain = ''.join(random.choices(string.ascii_lowercase, k=2))
		new_email = f"{'user'}|{random_string}@{domain}.{sub_domain}"
		# print("\n\n\n new_email >>>", new_email)

		# Update the 'x_accountemail' column in the DataFrame
		df.at[index, 'pmx_id'] = new_email
		df.at[index, 'email'] = f"{random_string}@{domain}.{sub_domain}"
		df.at[index, 'company_id'] = 2

df.to_csv(output_file_path, index=False, encoding='latin1')

print(f'Total count of the rows in non empty input CSV file: {row_count}')  # 1,74,184 rows
print(f'Total count of the columns in new input CSV file: {col_count}')  # 162 Columns

print("\n\n Output file ready ")
# Record the end time
end_time = time.time()

# Calculate the execution duration
execution_duration = end_time - start_time

# Convert execution time to minutes
execution_duration_minutes = execution_duration / 60
print(f'\nTime taken: {execution_duration_minutes:.2f} minutes')