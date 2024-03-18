# pmx-data-seed


# Import FS Client DATA

1) Create res company in your database as id = 2
2) Execute the "fs_client_header_map.py" script for remove empty columns from input file and map the header ftx to pmx
3) Execute the "fs_client_update_data.py" script for update email and pmx id data
4) Execute the "import_fs_client_xmlrpc.py" script for import the data into eu fs client data