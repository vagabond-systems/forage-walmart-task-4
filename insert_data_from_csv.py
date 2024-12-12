import sqlite3
import csv

# Connect to SQLite database
conn = sqlite3.connect("./shipment_database.db")
cursor = conn.cursor()

# Process shipping_data_0.csv
with open("./data/shipping_data_0.csv", 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row
    for row in reader:
        origin_warehouse = row[0]
        destination_store = row[1]
        product_name = row[2]
        product_quantity = int(row[4])

        # Insert product if not exists
        cursor.execute("INSERT OR IGNORE INTO product (name) VALUES (?)", (product_name,))
        conn.commit()
        cursor.execute("SELECT id FROM product WHERE name = ?", (product_name,))
        product_id_row = cursor.fetchone()
        if product_id_row:
            product_id = product_id_row[0]

            # Insert shipment
            cursor.execute("INSERT INTO shipment (product_id, quantity, origin, destination) VALUES (?, ?, ?, ?)",
                           (product_id, product_quantity, origin_warehouse, destination_store))
            conn.commit()

shipment_dictionary = {}

# Process shipping_data_1.csv
with open("./data/shipping_data_1.csv", 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row
    for row in reader:
        shipment_identifier = row[0]
        product_name = row[1]

        # Insert product if not exists
        cursor.execute("INSERT OR IGNORE INTO product (name) VALUES (?)", (product_name,))
        conn.commit()
        cursor.execute("SELECT id FROM product WHERE name = ?", (product_name,))
        product_id_row = cursor.fetchone()
        if product_id_row:
            product_id = product_id_row[0]

            if shipment_identifier in shipment_dictionary:
                if product_id in shipment_dictionary[shipment_identifier]:
                    shipment_dictionary[shipment_identifier][product_id] += 1
                else:
                    shipment_dictionary[shipment_identifier][product_id] = 1
            else:
                shipment_dictionary[shipment_identifier] = {product_id: 1}

# Process shipping_data_2.csv
with open("./data/shipping_data_2.csv", 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row
    for row in reader:
        shipment_identifier = row[0]
        origin_warehouse = row[1]
        destination_store = row[2]

        # Insert to database
        if shipment_identifier in shipment_dictionary:
            # insert one product for each shipment
            for product_id in shipment_dictionary[shipment_identifier]:        
                cursor.execute("INSERT INTO shipment (product_id, quantity, origin, destination) VALUES (?, ?, ?, ?)",
                               (product_id, shipment_dictionary[shipment_identifier][product_id], origin_warehouse, destination_store))
                conn.commit()

# Close the connection
conn.close()


# connect sqllite database "./shipment_database.db"

# shipping_data_0 = read "./data/shipping_data_0.csv"
# skip row 0, start from row 1
# for each row in shipping_data_0
#     # retrieve excel data
#     origin_warehouse = column 0
#     destination_store = column 1
#     product_name = column 2
#     product_quantity = column 4

#     # insert product if not exist    
#     insert(name) value (product_name) if not exist into product
#     product_id = select id from product where name = product_name

#     # insert shipment
#     insert(product_id, quantity, origin, destination) 
#     value (product_id, product_quantity, origin_warehouse, destination_store)
#     into shipment

# shipment_dictionary = {}

# shipping_data_1 = read "./data/shipping_data_1.csv"
# skip row 0, start from row 1
# for each row in shipping_data_1
#     # retrieve excel data
#     shipment_identifier = column 0
#     product_name = column 1
    
#     # insert product if not exist    
#     insert(name) value (product_name) if not exist into product
#     product_id = select id from product where name = product_name

    
#     if shipment_identifier in shipment_dictionary:
#         # add product to shipment
#         if product_id in shipment_dictionary[shipment_identifier]:
#             shipment_dictionary[shipment_identifier][product_id] += 1
#         else:
#             shipment_dictionary[shipment_identifier][product_id] = 1
#     else:
#         # create shipment
#         shipment_dictionary[shipment_identifier][product_id] = 1
        
    
# shipping_data_2 = read "./data/shipping_data_2.csv"
# skip row 0, start from row 1
# for each row in shipping_data_2
#     # retrieve excel data
#     shipment_identifier = column 0
#     origin_warehouse = column 1
#     destination_store = column 2

#     # insert to database
#     if shipment_identifier in shipment_dictionary:
#         # insert one product for each shipment
#         for product_id in shipment_dictionary[shipment_identifier]:
#             insert(product_id, quantity, origin, destination) 
#             value (product_id, shipment_dictionary[shipment_identifier][product_id], origin_warehouse, destination_store)
#             into shipment