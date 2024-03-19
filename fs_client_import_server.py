import xmlrpc.client
import csv
from datetime import datetime

# Define XML-RPC connection parameters
url = "http://private-test-pmx-coresystems-do-user-2412463-0.c.db.ondigitalocean.com"
db_name = "Master"
db_user = "odoo"
db_pwd = "AVNS_0byMv122u7uygOUkk4A"

# Establish XML-RPC connection
common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common", allow_none=True)
uid = common.authenticate(db_name, db_user, db_pwd, {})
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object", allow_none=True)

print("\nConnected to the database...")

# Path to the CSV file
csv_file = "x_fs_client_output.csv"

# Record the start time
start_time = datetime.now()


old_eu_fs_client_ids = models.execute_kw(
    db_name,
    uid,
    db_pwd,
    "eu.fs.client",
    "search_read",
    [[["old_eu_fs_client_id", "!=", False]]],
    {"fields": ["old_eu_fs_client_id"], "order": "id"},
)


old_eu_fs_client_ids = list(
    map(lambda old: old["old_eu_fs_client_id"], old_eu_fs_client_ids)
)


# Read data from CSV and insert into the database
with open(csv_file, "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    num = 0
    for row in reader:
        num += 1
        print(f" Row Number {num} & Row ID {row['id']}")

        if not row["id"].isdigit():
            continue

        row_id = int(row["id"])
        skip_id_list = [
            "171119",
            "171041",
            "151763",
            "151828",
            "151968",
            "152022",
            "170722",
            "151862",
            "171019",
            "170767",
            "170399",
            "170982",
            "171016",
            "170944",
            "151842",
        ]
        if row_id in skip_id_list or row_id in old_eu_fs_client_ids:
            continue
        row["old_eu_fs_client_id"] = row_id
        row["client_categorization"] = "retail"
        row["region"] = "EU"

        

        # Convert the company_id to integer if it's a string
        if isinstance(row['company_id'], str):
            row['company_id'] = int(row['company_id'])

        # Update the company_id
        if row['company_id'] == 5:
            row['company_id'] = 2
        elif row['company_id'] == 13:
            row['company_id'] = 1

      
        
        
        datetime_columns = ["cognito_result_date", "registration_date"]

        float_columns = [
            "total_collateral",
            "free_collateral",
            "unrealized_pnl",
            "realized_pnl",
            "annual_income",
            "apt_total_scoring_part_5",
            "apt_total_scoring_part_6",
            "apt_total_scoring_part_7",
            "apt_how_long_investing_score",
            "apt_experience_in_crypto_score",
            "apt_experience_in_derivatives_score",
            "apt_share_etf_experience_score",
            "apt_max_amount_to_trade_score",
            "apt_limit_loss_while_trading_score",
            "apt_pending_order_1_score",
            "apt_account_purpose_score",
            "apt_position_duration_score",
            "apt_risk_reward_ratio_score",
            "apt_risk_profile_score",
            "apt_knowledge_and_experience",
            "apt_product_governance",
            "apt_investing_in_financial_products_score",
            "apt_legal_entitys_risk_tolerance_score",
            "apt_derivatives_investment_last_year_score",
            "apt_leveraged_investment_last_year_score",
        ]

        int_columns = [
            "risk_score",
            "result_for_residence_country",
            "result_for_sanction_list",
            "result_for_industry",
            "result_for_mifid",
            "result_for_lead_origin",
            "result_for_affiliation",
            "result_for_pep",
        ]

        for col, value in row.items():
            if value == "":
                row[col] = None
            elif col in float_columns:
                row[col] = float(str(value)) if value else 0.0
            elif col in int_columns:
                row[col] = int(float(value)) if value else ""
            elif col == "first_name":
                row[col] = value if value is not None and value != "" else "Unknown"
            elif col in datetime_columns:
                try:
                    format_str = "%Y-%m-%d %H:%M:%S"
                    row[col] = datetime.strptime(value, format_str) if value else None
                except ValueError:
                    print(
                        f"Error parsing date for column '{col}' with value '{value}'. Check the date format."
                    )
                    row[col] = None
          
        # Insert row into the database
        try:
            models.execute_kw(
                db_name, uid, db_pwd, "eu.fs.client", "create", [row]
            )
            fs_client_id = models.execute_kw(
                db_name,
                uid,
                db_pwd,
                "fs.client",
                "search_read",
                [[["pmx_id", "=", row["pmx_id"]]]],
                {"fields": ["pmx_id"]},
            )
            if fs_client_id:
                fs_client_id = models.execute_kw(
                    db_name,
                    uid,
                    db_pwd,
                    "fs.client",
                    "write",
                    [[fs_client_id[0]["id"]], {"old_fs_client_id": row_id}],
                )
        except Exception as e:
            print(f"Error inserting row: {e}")


# Record the end time
end_time = datetime.now()

# Calculate the execution duration
execution_duration = end_time - start_time

print(f"Time taken: {execution_duration.total_seconds():.2f} seconds")
