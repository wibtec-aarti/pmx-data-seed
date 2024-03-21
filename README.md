# pmx-data-seed


# Import FS Client DATA

1) Create two compnay FTX Europe and FTX Switzerland GmbH
2) Execute the "fs_client_header_map.py" script for remove empty columns from input file and map the header ftx to pmx
3) Execute the "fs_client_update_data.py" script for update email and pmx id data
4) Execute the "fs_client_import_local.py" script for import data into local
4) Execute the "fs_client_import_server.py" script for import data into server

# Skip Fs Client ids 
56 Record: ['171119','171041','151763','151828','151968','152022','170722','151862','171019','170767','170399','170982','171016','170944','151842'] 

# Tabel and count of records
1) FS Client : 1,74,184: Done
2) Trades: 2,05,12,688: In Progress
3) Deposite: 3,41,843 :
4) Withdraws: 1,70,823 :
5) Balance: 2,21,01,978 :
6) Positions: 1,82,96,479 :

# Trades 
1) Execute the trade_import_local.py file local db 
2) Execute the trade_import_server.py file server db  
