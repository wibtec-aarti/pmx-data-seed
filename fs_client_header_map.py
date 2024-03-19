import pandas as pd
import time
import random
import string

# Record the start time
start_time = time.time()

input_file_path = "x_fs_client.csv"
update_file_path = "x_fs_client_update.csv"

row_count = 0
col_count = 0
new_row_count = 0
new_col_count = 0

chunk_size = 100000  # Adjust the chunk size based on your available memory
empty_columns_set = set()  # Defined empty cols set

for chunk in pd.read_csv(
    input_file_path, encoding="utf-8", chunksize=chunk_size, low_memory=False
):
    row_count += len(chunk)
    col_count = max(col_count, len(chunk.columns))

    # Check for empty columns
    empty_columns_set.update(chunk.columns[chunk.isnull().all()])

non_empty_columns_set = set(chunk.columns) - empty_columns_set

print("\n************ Original Input CSV file ************\n")

print(f"Total count of the rows in original input CSV file: {row_count}")  # 174184 rows
print(f"Total count of the columns in original input CSV file: {col_count}")  # 328 row
print(f"\n\n\n non_empty  columns in original input CSV file: {len(non_empty_columns_set)}")  #225
print(f"\n\n\n empty_columns_set  columns in original input CSV file: {len(empty_columns_set)}") #103   

df = pd.DataFrame()
for new_chunk in pd.read_csv(
    input_file_path,
    encoding="utf-8",
    chunksize=chunk_size,
    usecols=non_empty_columns_set,
    low_memory=False,
):
    df = pd.concat([df, new_chunk], ignore_index=True)
    
# Get count of keep colum list in csv file 
columns_to_keep = [
    "id",
    "create_date",
    "write_date",
    "x_firstname",
    "x_lastname",
    "x_FTXID",
    "x_address",
    "x_nationality",
    "x_CountryofResidence",
    "x_email",
    "x_state_province_region",
    "x_IDDocumentCountry",
    "x_dateofbirth",
    "x_placeofbirth",
    "x_riskscore",
    "x_phonenumber",
    "x_middlename",
    "x_IDNumber",
    "x_city",
    "x_postalcode",
    "x_other_source_of_income",
    "x_tax_jurisdiction",
    "x_total_collateral",
    "x_free_collateral",
    "x_unrealized_PnL",
    "x_realized_PnL",
    "x_IDDocumentBack_URL",
    "x_IDDocumentFront_URL",
    "x_PhotoProof_URL",
    "x_ProofofAddress_URL",
    "x_onboardingNotes",
    "x_clientType",
    "x_closeDate",
    "x_resultForSanctionList",
    "x_resultForResidenceCountry",
    "x_resultForIndustry",
    "x_resultForMifid",
    "x_resultForLeadOrigin",
    "x_resultForAffiliation",
    "x_resultForPep",
    "x_proofofIdentityDocNum",
    "x_proofOfIdentityIssueDate",
    "x_proofOfIdentityIssuePlace",
    "x_proofOfResidenceIssuePlace",
    "x_currency",
    "x_studio_type_of_entity",
    "x_studio_entity_registered_name",
    "x_studio_main_activity_of_the_entity",
    "x_studio_country_of_registration",
    "x_studio_registration_number",
    "x_studio_registered_address",
    "x_studio_head_office_address",
    "x_studio_operating_agreement_1_filename",
    "x_annual_income",
    "x_employer",
    "x_other_industry",
    "x_total_scoring_part7",
    "x_total_scoring_part5",
    "x_total_scoring_part6",
    "x_company_id",
    "x_cognito_result",
    "x_cognito_ref",
    "x_studio_experience_in_derivates_score",
    "x_studio_share_etf_experience_score",
    "x_studio_limit_loss_while_trading_score",
    "x_studio_accountpurpose_score",
    "x_studio_position_duration_score",
    "x_studio_riskprofile_score",
    "x_knowledge_and_experience",
    "x_product_governance",
    "x_jumioresult",
    "x_studio_new_sanctionhit",
    "x_studio_active",
    "x_selfie_URL",
    "x_striperesult",
    "x_cognito_result_date",
    "x_ftx_kyc_link",
    "x_street1",
    "x_street2",
    "x_cognito_screening_url",
    "x_rejectedDate",
    "x_stripe_url_result",
    "x_investing_in_financial_products_score",
    "x_legal_entitys_risk_tolerance_score",
    "x_derivatives_investment_last_year_score",
    "x_leveraged_investment_last_year_score",
    "x_registration_date",
    "x_trulioo_transaction_id",
    "x_trulioo_link",
    "x_trulioo_json_result",
    "x_full_legal_name",
    "x_trulioo_errors",
    "x_kyc_drive",
    "x_boCheckProcessId",
]

print(f"\n\n Count of the columns_to_keep : {len(columns_to_keep)}")
df = df[columns_to_keep]

#Map colum with pmx
pmx_header = [
    "id",
    "create_date",
    "write_date",
    "first_name",
    "last_name",
    "pmx_id",
    "address",
    "nationality",
    "country_of_residence",
    "email",
    "state_province_region",
    "id_document_country",
    "date_of_birth",
    "place_of_birth",
    "risk_score",
    "phone_number",
    "middle_name",
    "id_number",
    "city",
    "postal_code",
    "ecop_other_source_of_income",
    "tax_jurisdiction",
    "total_collateral",
    "free_collateral",
    "unrealized_pnl",
    "realized_pnl",
    "id_document_back_url",
    "id_document_front_url",
    "photo_proof_url",
    "proof_of_address_url",
    "onboarding_notes",
    "client_type",
    "close_date",
    "result_for_sanction_list",
    "result_for_residence_country",
    "result_for_industry",
    "result_for_mifid",
    "result_for_lead_origin",
    "result_for_affiliation",
    "result_for_pep",
    "proof_of_identity_doc_num",
    "proof_of_identity_issue_date",
    "proof_of_identity_issue_place",
    "proof_of_residence_issue_place",
    "currency",
    "type_of_entity",
    "entity_registered_name",
    "main_activity_of_the_entity",
    "country_of_registration",
    "registration_number",
    "registered_address",
    "head_office_address",
    "operating_agreement_1_filename",
    "annual_income",
    "ecop_employer",
    "other_industry",
    "apt_total_scoring_part_7",
    "apt_total_scoring_part_5",
    "apt_total_scoring_part_6",
    "company_id",
    "cognito_result",
    "cognito_ref",
    "apt_experience_in_derivatives_score",
    "apt_share_etf_experience_score",
    "apt_limit_loss_while_trading_score",
    "apt_account_purpose_score",
    "apt_position_duration_score",
    "apt_risk_profile_score",
    "apt_knowledge_and_experience",
    "apt_product_governance",
    "jumio_result",
    "new_sanction_hit",
    "active",
    "selfie_url",
    "stripe_result",
    "cognito_result_date",
    "kyc_link",
    "street_1",
    "street_2",
    "cognito_screening_url",
    "reject_date",
    "stripe_url_result",
    "apt_investing_in_financial_products_score",
    "apt_legal_entitys_risk_tolerance_score",
    "apt_derivatives_investment_last_year_score",
    "apt_leveraged_investment_last_year_score",
    "registration_date",
    "trulioo_experience_transaction_id",
    "trulioo_link",
    "trulioo_json_result",
    "full_legal_name",
    "trulioo_errors",
    "kyc_drive",
    "bo_check_process_id",
]
print(f"\n\n Count of the pmx_header : {len(pmx_header)}")
df.columns = pmx_header

# Write update header in to file 
df.to_csv(update_file_path, index=False, encoding="utf-8")

print("\n************ Header Updated CSV file ************\n")

print(f"Total count of the rows in update CSV file: {len(df)}")  
print(f"Total count of the columns in update CSV file: {len(df.columns)}")  

# Record the end time
end_time = time.time()

# Calculate the execution duration
execution_duration = end_time - start_time

# Convert execution time to minutes
execution_duration_minutes = execution_duration / 60
print(f"\nTime taken: {execution_duration_minutes:.2f} minutes")
