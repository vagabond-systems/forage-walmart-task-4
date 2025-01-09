import csv
import sqlite3

con = sqllite3.connect('shippingdata.db')

with open('data/shipping_data_0.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        print(row)

print("hello")