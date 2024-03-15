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

dbname = "pmx_16_new"

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

        for row in reader:
            print("Row: ",row,'\n')
        
            if not row['id'].isdigit():
                continue  # Skip records with invalid 'id'

            num += 1
            print(f" Row Number {num} & Trade record ID {row['id']}" )

            trade_pmx_vals = {
                "id":row["id"],
                "create_uid":row["create_uid"],
                "create_date":row["create_date"],
                "write_uid":row["write_uid"],
                "write_date":row["write_date"],
                "username":row["x_name"],
                "pmx_id":row["x_FTXID"],
                "account_id":row["x_accountid"],
                "commission":row["x_commission"],
                "commission_euro":row["x_commissioneur"],
                "created_at":row["x_datetime"],
                "fee":row["x_fee"],
                "fee_rate":row["x_feerate"],
                "price":row["x_price"],
                "matched_quantity":row["x_quantity"],
                "size":row["x_size"],
                "time":row["x_time"],
                "order_matching_engine_id":row["x_tradeid"],
                "matching_engine_timestamp":row["x_tradetime"],
                "trade_type":row["x_type"],
                "fs_client_id":row["x_client"],
                "currency_id":row["x_currency_id"],
                "country_of_residence":row["x_studio_country_of_residence"],
                "active":row["x_studio_active"],
                "company_id":row["x_company_id"],
                "fee_amount":row["x_feeamount"],
                "fee_amount_euro":row["x_feeamounteuro"],
                "trade_type":row["x_studio_trade_type_id"],
                }

            trade_columns = tuple(trade_pmx_vals.keys())
            print("Trade columns: ",trade_columns,'\n')

            trade_values = tuple(trade_pmx_vals.values())
            print("Trade values: ",trade_values,'\n')

            insert_sql = sql.SQL(f"INSERT INTO trade {trade_columns} VALUES {trade_values} RETURNING id")

            print("Insert sql: ",insert_sql)

            # cur.execute(insert_sql)



        
#             row['client_categorization'] = 'retail'
            
#             datetime_columns = [
#                 'cognito_result_date', 
#                 'registration_date',
#             ]
            
#             float_columns = [
#                 'total_collateral',
#                 'free_collateral',
#                 'unrealized_pnl',
#                 'realized_pnl',
#                 'annual_income',
#                 'apt_total_scoring_part_5',
#                 'apt_total_scoring_part_6',
#                 'apt_total_scoring_part_7',
#                 'apt_how_long_investing_score',
#                 'apt_experience_in_crypto_score',
#                 'apt_experience_in_derivatives_score',
#                 'apt_share_etf_experience_score',
#                 'apt_max_amount_to_trade_score',
#                 'apt_limit_loss_while_trading_score',
#                 'apt_pending_order_1_score',
#                 'apt_account_purpose_score',
#                 'apt_position_duration_score',
#                 'apt_risk_reward_ratio_score',
#                 'apt_risk_profile_score',
#                 'apt_knowledge_and_experience',
#                 'apt_product_governance',
#                 'apt_investing_in_financial_products_score',
#                 'apt_legal_entitys_risk_tolerance_score',
#                 'apt_derivatives_investment_last_year_score',
#                 'apt_leveraged_investment_last_year_score',
#             ]

#             int_columns = [
#                 'risk_score', 
#                 'result_for_residence_country',
#                 'result_for_sanction_list',
#                 'result_for_industry',
#                 'result_for_mifid',
#                 'result_for_lead_origin',
#                 'result_for_affiliation',
#                 'result_for_pep',
#             ]
            
#             for col, value in row.items():
#                 if value == '':
#                     row[col] = None
#                 elif col in float_columns:
#                     row[col] = float(str(value)) if value else 0.0
#                 elif col in int_columns:
#                     row[col] = int(float(value)) if value else ''
#                 elif col == 'first_name':
#                     row[col] = value if value is not None and value != '' else 'Unknown'
#                 elif col in datetime_columns:
#                     try:
#                         format_str = '%Y-%m-%d %H:%M:%S' 
#                         row[col] = datetime.strptime(value, format_str) if value else None
#                     except ValueError:
#                         print(f"Error parsing date for column '{col}' with value '{value}'. Check the date format.")
#                         row[col] = None 
    

#             # Execute the INSERT statement with the values from the current row
#             cur.execute(insert_sql, list(row.values()))
#             # inserted_id = cur.fetchone()[0]
            
#             if num % batch_size == 0:
#                 conn.commit()

#     print(f" {num} Data imported successfully!")
#     print(f" {batch_size} batch_size!")

#     # Record the end time
#     end_time = time.time()

#     # Calculate the execution duration
#     execution_duration = end_time - start_time
    
#     # Convert execution time to minutes
#     execution_duration_minutes = execution_duration / 60
#     print(f'Time taken: {execution_duration_minutes:.2f} minutes')

except psycopg2.Error as e:
    print(f"Error: Unable to import data\n{e}")

finally:
    # Close the cursor and connection when done
    if conn is not None:
        conn.commit()
        cur.close()
        conn.close()
        print("\n\n\n Connection closed.")














 '''
        Row:  {'id': '659700', 'create_uid': '10', 'create_date': '2022-04-21 21:02:42.073432', 
        'write_uid': '117', 'write_date': '2022-09-02 17:22:01.230518', 'x_name': 'x_trades7071448906', 
        'x_studio_fs_client': '', 'x_studio_date_time': '', 'x_studio_field_mNIxp': '', 
        'x_studio_isin': '', 'x_studio_direction': '', 'x_studio_quantity': '', 'x_studio_trade_price': '', 
        'x_studio_trade_currency': '', 'x_studio_trade_exchange_rate': '', 'x_studio_commission_eur': '', 
        'x_studio_net_amount_eur': '', 'x_studio_instrument_name': '', 'x_studio_contractobjectid': '', 
        'x_studio_transferid': '', 'x_FTXID': 'x_trades7071448906', 'x_accountid': '98267264', 
        'x_basecurrency': '', 'x_commission': '', 'x_commissioncurrency': '', 'x_commissioneur': '', 
        'x_counterparty': 'FTX Switzerland GmbH', 'x_datetime': '2022-03-11 23:04:14', 'x_fee': '0.01308125', 
        'x_feecurrency': 'USDC', 'x_feerate': '0.0007', 'x_freecollateral': '', 'x_future': 'SOL-PERP', 
        'x_instrument': '', 'x_leverage': '', 'x_liquidity': 'taker', 'x_market': 'SOL-PERP', 
        'x_orderid': '127958761246', 'x_price': '81.25', 'x_quantity': '18.6875', 'x_quotecurrency': '', 
        'x_side': 'buy', 'x_size': '0.23', 'x_customerid': '', 'x_subaccount': '', 'x_time': '', 
        'x_totalcollateral': '', 'x_tradeid': '3507191322', 'x_tradeliquidation': 'f', 'x_tradeside': 'buy', 
        'x_tradesize': '11.73', 'x_tradetime': '2022-03-11 23:04:14', 'x_tradecurrency': '', 
        'x_tradeprice': '82.8725', 'x_type': 'market', 'x_direction': '', 'x_accountemail': 'user|ednor-sefaj@web.de', 
        'x_client': '1403', 'x_fs_client_buyer_id': '1403', 'x_currency_id': '', 
        'x_amount_currency': '17.124072207459', 'x_amount': '18.6875', 'x_studio_country_of_residence': 'DEU', 
        'x_studio_active': 't', 'x_studio_archived_date': '', 'x_company_id': '5', 'x_studio_client_not_set': '', 
        'x_feeamount': '0.013081249999999999', 'x_feeamounteuro': '0.011998945147679324', 'x_studio_trade_type_id': '', 
        'x_studio_clients_company': '5', 'x_instrument_rate': '81.25', 'x_archived_client_id': ''}
        '''
