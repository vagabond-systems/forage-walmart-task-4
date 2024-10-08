import sqlite3
import csv
import os

# Get the absolute path of the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
# Database file path
db_file = os.path.join(script_dir, 'shipment_database.db')

# CSV file path
csv_file = os.path.join(script_dir, 'shipping_data_0.csv')

# Connect to the database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Read data from CSV file and insert into tables
with open(csv_file, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # Extract relevant data from the CSV row
        origin = row['origin_warehouse']
        destination = row['destination_store']
        product_name = row['product']
        quantity = int(row['product_quantity'])

        # Insert product into the "product" table
        # Check if the product already exists
        cursor.execute('SELECT id FROM product WHERE name = ?', (product_name,))
        existing_product = cursor.fetchone()

        if existing_product is None:
    # Product doesn't exist, insert it
            cursor.execute('INSERT INTO product (name) VALUES (?)', (product_name,))
            product_id = cursor.lastrowid
        else:
    # Product already exists, use its existing ID
            product_id = existing_product[0]

        # Insert shipment into the "shipment" table
        cursor.execute('INSERT INTO shipment (product_id, quantity, origin, destination) VALUES (?, ?, ?, ?)',
             (product_id, quantity, origin, destination))

# Commit the changes and close the connection
conn.commit()
conn.close()