import csv
import logging
import time
from datetime import datetime

import psycopg2
from psycopg2 import sql

logging.basicConfig(level=logging.INFO)

# Connection parameters
dbname = "v16e-pmx-test"

# Path to the CSV file
csv_file = "x_fs_client_output.csv"

# Record the start time
start_time = time.time()
batch_size = 2000

# Establish connection to the database
try:
    conn = psycopg2.connect(dbname=dbname)
    logging.info("Connection established successfully!")
    # Create a cursor
    cur = conn.cursor()
    # Read data from CSV and insert into the database
    with open(csv_file, "r", encoding="latin1") as file:
        reader = csv.DictReader(file)
        num = 0
        skip_id_list = [
            '171119', '171041', '151763', '151828', '151968',
            '152022', '170722', '151862', '171019', '170767',
            '170399', '170982', '171016', '170944', '151842'
        ]

        for row in reader:
            if row['id'] in skip_id_list:
                continue
            if not row['id'].isdigit():
                continue  # Skip records with invalid 'id'

            num += 1
            print(f" Row Number {num} & Row ID {row['id']}")

            # EM
            # country_code/country_of_residence and company_code
            row['client_categorization'] = 'retail'
            # row['region'] = 'EU' #char
            # row['company_id'] = 2 #M2O

            datetime_columns = [
                'cognito_result_date',
                'registration_date',
            ]

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

            insert_sql = sql.SQL("INSERT INTO fs_client ({}) VALUES ({}) RETURNING id").format(
                sql.SQL(', ').join(map(sql.Identifier, row.keys())),
                sql.SQL(', ').join(sql.Placeholder() * len(row))
            )

            # Execute the INSERT statement with the values from the current row
            cur.execute(insert_sql, list(row.values()))
            # inserted_id = cur.fetchone()[0]
            #
            # if num == 5:
            #     break

            if num % batch_size == 0:
                conn.commit()

    print(f" {num} Data imported successfully!")
    print(f" {batch_size} batch_size!")

    # Record the end time
    end_time = time.time()

    # Calculate the execution duration
    execution_duration = end_time - start_time

    # Convert execution time to minutes
    execution_duration_minutes = execution_duration / 60
    print(f'Time taken: {execution_duration_minutes:.2f} minutes')

except psycopg2.Error as e:
    print(f"Error: Unable to import data\n{e}")

finally:
    # Close the cursor and connection when done
    if conn is not None:
        conn.commit()
        cur.close()
        conn.close()
        print("\n\n\n Connection closed.")
