import pymongo
from pymongo import MongoClient
import re
# some of the used methods:
# insert_one
# insert_many
# update_one
# update_many
# delete_one
# delete_many
# find_one
# find
# sort

client = MongoClient('mongodb://localhost:27017')
db = client.mytest

# INSERT
print("INSERT --------------------------------------------------------------")
result = db.customers.insert_one({'name':'Natasha', 'age':25})
print("inserted id %s" % (result.inserted_id))
customer1 = { 'name' : 'Natasha 2', 'age': 22 }
customer2 = { 'name' : 'Natasha 3', 'age': 33 }
result = db.customers.insert_many([customer1, customer2])
print("inserted ids %s" % (result.inserted_ids))
db.customers.insert_one({'name':'Natasha', 'age':26})

# FIND
print("FIND --------------------------------------------------------------")
print("all documents in 'customers' collection:")
for doc in db.customers.find():
    print(doc)
# print documents sorted
print('Print the documents sorted by name and _id:')
for doc in db.customers.find().sort([('name', pymongo.ASCENDING), 
                                     ('_id', pymongo.ASCENDING)]):
    print(doc)
# search using regex (similar to select ... from ... where like '%...%')
print('Search for names containing \'tasha\' followed by digit:')
for doc in db.customers.find(
    {'name': re.compile('^.*tasha.*\d$', re.IGNORECASE)}).sort('name'):
    print(doc)

# UPDATE
print("UPDATE --------------------------------------------------------------")
print("initially: %s" % 
      ([doc for doc in db.customers.find({'name':re.compile('^Natasha.*')}, 
                                         {'_id' : 0})]))
db.customers.update_one({'name':'Natasha'}, {'$set': {'age':30}})
print("after update_one: %s" % ([doc for doc in db.customers.find(
    {'name':re.compile('^Natasha.*')}, {'_id' : 0})]))
db.customers.update_many({'name':'Natasha'}, {'$set': {'age':23}})
print("after update_many: %s" % ([doc for doc in db.customers.find(
    {'name':re.compile('^Natasha.*')}, {'_id' : 0})]))

# DELETE
print("DELETE --------------------------------------------------------------")
result = db.customers.delete_one(
    {'name': re.compile('^.*tasha.*$', re.IGNORECASE)})
print('Deleted one document with name containing \'tasha\'')
print("deleted %s document" % (result.deleted_count))
print('The raw result document returned by the server: %s' % 
      (result.raw_result) )
result = db.customers.delete_many(
    {'name': re.compile('^.*tasha.*$', re.IGNORECASE)})
print('Deleted all documents with names containing \'tasha\'')
print("deleted %s documents" % (result.deleted_count))
print('The raw result document returned by the server: %s' % 
      (result.raw_result) )
filter = {'age':{'$gte':10, '$lte':55}}
print("filter: %s found %s documents" % 
      (filter, db.customers.find(filter).count()))
print("find_one(%s) returned: %s" % (filter, db.customers.find_one(filter)))
# the only mix in projection possible is '_id':0 and some field(s) : 1
print([doc for doc in db.customers.find({}, 
                                        {'name' : 1, '_id' : 0})])
