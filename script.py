import pandas as pd
import sqlite3

#----CSV Files paths
csv_file_0 = 'data/shipping_data_0.csv'
csv_file_1 = 'data/shipping_data_1.csv'
csv_file_2 = 'data/shipping_data_2.csv'
database_path = 'shipment_database.db'

#----Reading CSV files into DataFrames
df_main = pd.read_csv(csv_file_0)
df_ship_info = pd.read_csv(csv_file_1)
df_ship_loc = pd.read_csv(csv_file_2)

#---Printing columns to debug
print("Columns in df_ship_info:", df_ship_info.columns)
print("Columns in df_ship_loc:", df_ship_loc.columns)

#---Combining data from shipping_data_1.csv and shipping_data_2.csv
df_combined = pd.merge(df_ship_info, df_ship_loc, on='shipment_identifier')

#---Calculating total quantities for each shipment
df_combined['total_qty'] = df_combined.groupby('shipment_identifier')['product'].transform('size')

#---Connecting to SQLite database
connection = sqlite3.connect(database_path)
cur = connection.cursor()

#---Creating table shipping_data_1 if not exists
cur.execute('''
    CREATE TABLE IF NOT EXISTS shipping_data_1 (
        shipment_identifier TEXT,
        product TEXT,
        on_time TEXT,
        origin_warehouse TEXT,
        destination_store TEXT,
        driver_identifier TEXT,
        quantity INTEGER
    )
''')

#--Inserting data from shipping_data_0.csv
df_main.to_sql('shipping_data_0', connection, if_exists='append', index=False)

#---Inserting combined data into shipping_data_1 table
for idx, record in df_combined.iterrows():
    cur.execute("""
    INSERT INTO shipping_data_1 (shipment_identifier, product, on_time, origin_warehouse, destination_store, driver_identifier, quantity)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (record['shipment_identifier'], record['product'], record['on_time'], record['origin_warehouse'], record['destination_store'], record['driver_identifier'], record['total_qty']))

#---Commiting and close connection
connection.commit()
connection.close()

print("Data inserted successfully!")
