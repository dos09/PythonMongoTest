import sys
import pymongo
from pymongo import MongoClient

print("sys.prefix: " + sys.prefix)

#client = MongoClient() # with the default host and port
#client = MongoClient('localhost', 27017)
client = MongoClient('mongodb://localhost:27017')
# db = client["mytest"] 
db =  client.mytest# if the database doesn't exist it is automatically specified
# db.customers
# db['customers']
for doc in db.customers.find():
    print(doc['name'])
    print(doc)