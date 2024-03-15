import xmlrpc.client
import csv
from datetime import datetime

# Define XML-RPC connection parameters
url = "http://localhost:8069/"
db_name = "v16_PMX_07_03_2024"
db_user = "admin"
db_pwd = "admin"

# Establish XML-RPC connection
common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common",allow_none=True)
uid = common.authenticate(db_name, db_user, db_pwd, {})
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object",allow_none=True)

print("\nConnected to the database...")

# Path to the CSV file
csv_file = "/home/odoo/Documents/wibtec/PP-1231/fs_client/x_fs_client_output.csv"

# Record the start time
start_time = datetime.now()

# Read data from CSV and insert into the database
with open(csv_file, "r", encoding="latin1") as file:
	reader = csv.DictReader(file)
	num = 0
	for row in reader:
		num += 1
		print(f" Row Number {num} & Row ID {row['id']}" )
		
		print("row.get('pmx_id')>>>", row.get('pmx_id'))
		
		# Set default values for specific columns
		row['client_categorization'] = 'retail'
		
		country_of_residence = row.get('country_of_residence')
		
		row['region'] = 'EU'
		

		skip_id_list = ['171119','171041','151763','151828','151968',
			'152022','170722','151862','171019','170767','170399','170982','171016',
			'170944','151842']

		
		if row['id'] in skip_id_list:
			continue

		if not row['id'].isdigit():
			continue  # Skip records with invalid 'id'
	
		datetime_columns = ['cognito_result_date', 'registration_date']
			
		float_columns = [
			'total_collateral',
			'free_collateral',
			'unrealized_pnl',
			'realized_pnl',
			'annual_income',
			'apt_total_scoring_part_5',
			'apt_total_scoring_part_6',
			'apt_total_scoring_part_7',
			'apt_how_long_investing_score',
			'apt_experience_in_crypto_score',
			'apt_experience_in_derivatives_score',
			'apt_share_etf_experience_score',
			'apt_max_amount_to_trade_score',
			'apt_limit_loss_while_trading_score',
			'apt_pending_order_1_score',
			'apt_account_purpose_score',
			'apt_position_duration_score',
			'apt_risk_reward_ratio_score',
			'apt_risk_profile_score',
			'apt_knowledge_and_experience',
			'apt_product_governance',
			'apt_investing_in_financial_products_score',
			'apt_legal_entitys_risk_tolerance_score',
			'apt_derivatives_investment_last_year_score',
			'apt_leveraged_investment_last_year_score',
		]

		int_columns = [
			'risk_score', 
			'result_for_residence_country',
			'result_for_sanction_list',
			'result_for_industry',
			'result_for_mifid',
			'result_for_lead_origin',
			'result_for_affiliation',
			'result_for_pep',
		]
			
		for col, value in row.items():
			if value == '':
				row[col] = None
			elif col in float_columns:
				row[col] = float(str(value)) if value else 0.0
			elif col in int_columns:
				row[col] = int(float(value)) if value else ''
			elif col == 'first_name':
				row[col] = value if value is not None and value != '' else 'Unknown'
			elif col in datetime_columns:
				try:
					format_str = '%Y-%m-%d %H:%M:%S' 
					row[col] = datetime.strptime(value, format_str) if value else None
				except ValueError:
					print(f"Error parsing date for column '{col}' with value '{value}'. Check the date format.")
					row[col] = None 

		# Insert row into the database
		try:
			eu_fs_client_id = models.execute_kw(db_name, uid, db_pwd, "eu.fs.client", "create", [row])
			print("\nCreated eu fs_client_id:", eu_fs_client_id)
		except Exception as e:
			print(f"Error inserting row: {e}")

		if num == 1:
			break
# Record the end time
end_time = datetime.now()

# Calculate the execution duration
execution_duration = end_time - start_time

print(f'Time taken: {execution_duration.total_seconds():.2f} seconds')
