from pymongo import MongoClient
import pymongo
import re

def printCustomers(info=None):
    if info != None:
        print(info)
    print([doc for doc in db.customers.find({}, {'_id':0, 'name':1, 'age':1})])

client = MongoClient('mongodb://localhost:27017')
db = client.mytest

# bulk inserts

bulk = db.customers.initialize_ordered_bulk_op()
bulk.insert({'name': 'XXXASD_1', 'age': 1})
bulk.insert({'name': 'XXXASD_2', 'age': 2})
bulk.insert({'name': 'XXXASD_3', 'age': 3})
bulk.insert({'name': 'XXX__ASD_4', 'age': 4})
bulk.insert({'name': 'XXX__ASD_5', 'age': 5})
result = bulk.execute()
print('result(inserts): %s' % (result))
printCustomers('after bulk inserts:')

regexXXXAll = re.compile('XXX.*', re.IGNORECASE)
regexXXX__ = re.compile('XXX__.*', re.IGNORECASE)

# bulk updates ( + replace_one )

bulk = db.customers.initialize_ordered_bulk_op()
bulk.find({'name': regexXXXAll}).update_one({'$set': {'name': 'XXX_X'}})
bulk.find({'name': regexXXX__}).update({'$inc': {'age':10}})  # increment by 10
bulk.find({'name': 'NoSuchName'}).upsert().update_one({'$set':{'name': 'Gencho', 'age':90}})
bulk.find({'name': 'XXXASD_2'}).replace_one({'name':'XXX2222', 'age': 'banana'})
result = bulk.execute()
print('result(updates): %s' % (result))
printCustomers('after bulk updates:')

# bulk removals

bulk = db.customers.initialize_ordered_bulk_op()
bulk.find({'name': 'Gencho'}).remove_one()
bulk.find({'name': regexXXXAll}).remove()
result = bulk.execute()
print('result(removals): %s' % (result))
printCustomers('after bulk removals:')
