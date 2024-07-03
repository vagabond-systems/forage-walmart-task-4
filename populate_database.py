import pandas as pd
import sqlite3

#----CSV Files paths
csv_file_0 = 'data/shipping_data_0.csv'
csv_file_1 = 'data/shipping_data_1.csv'
csv_file_2 = 'data/shipping_data_2.csv'
database_path = 'shipment_database.db'

#----Reading CSV files into DataFrames
df0 = pd.read_csv(csv_file_0)
df1 = pd.read_csv(csv_file_1)
df2 = pd.read_csv(csv_file_2)

#---Printing columns to debug
print("Columns in df1:", df1.columns)
print("Columns in df2:", df2.columns)

#---Combining data from shipping_data_1.csv and shipping_data_2.csv
df_combined = pd.merge(df1, df2, on='shipment_identifier')

#---Calculating total quantities for each shipment
df_combined['total_quantity'] = df_combined.groupby('shipment_identifier')['product'].transform('size')

#---Connecting to SQLite database
conn = sqlite3.connect(database_path)
cursor = conn.cursor()

#---Creating table shipping_data_1 if not exists
cursor.execute('''
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
df0.to_sql('shipping_data_0', conn, if_exists='append', index=False)

#---Inserting combined data into shipping_data_1 table
for index, row in df_combined.iterrows():
    cursor.execute("""
    INSERT INTO shipping_data_1 (shipment_identifier, product, on_time, origin_warehouse, destination_store, driver_identifier, quantity)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (row['shipment_identifier'], row['product'], row['on_time'], row['origin_warehouse'], row['destination_store'], row['driver_identifier'], row['total_quantity']))

#---Commiting and close connection
conn.commit()
conn.close()

print("Data inserted successfully!")
