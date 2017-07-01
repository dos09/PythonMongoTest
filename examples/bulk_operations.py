from pymongo import MongoClient
import pymongo
import re

def printCustomers(info=None):
    if info is not None:
        print(info)
    print([doc for doc in db.customers.find({}, {'_id':0, 'name':1, 'age':1})])

regexXXXAll = re.compile('XXX.*', re.IGNORECASE)
regexXXX__ = re.compile('XXX__.*', re.IGNORECASE)

def insert(db):
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

def update(db):
    # bulk updates ( + replace_one )
    bulk = db.customers.initialize_ordered_bulk_op()
    bulk.find({'name': regexXXXAll}).update_one({'$set': {'name': 'XXX_X'}})
    # increment by 10 (all matched)
    bulk.find({'name': regexXXX__}).update({'$inc': {'age':10}})
    bulk.find({'name': 'NoSuchName'}).upsert().update_one(
        {'$set':{'name': 'Gencho', 'age':90}})
    bulk.find({'name': 'XXXASD_2'}).replace_one(
        {'name':'XXX2222', 'age': 'banana'})
    result = bulk.execute()
    print('result(updates): %s' % (result))
    printCustomers('after bulk updates:')

def delete(db):
    # bulk removals    
    bulk = db.customers.initialize_ordered_bulk_op()
    bulk.find({'name': 'Gencho'}).remove_one()
    bulk.find({'name': regexXXXAll}).remove()
    result = bulk.execute()
    print('result(removals): %s' % (result))
    printCustomers('after bulk removals:')
    
def addToSet_setOnInsert(db):
    bulk = db.customers.initialize_ordered_bulk_op()
    name = 'Lala'
    letters = ['a', 'b', 'c']
    bulk.insert({ 'name' : name, 'letters' : ['a', 'b'] })
    bulk.find({'name': name}).update_one(
        {
            '$addToSet': { 'letters': { '$each': letters } }
        }
    )
    bulk.find({ 'name': 'NoSuchThing' }).upsert().update_one(
        {
            '$addToSet': { 'letters': { '$each': letters } },
            '$setOnInsert': { 'info': 'I was set on insert!' }
        }
    )
    bulk.execute()
        
client = MongoClient('mongodb://localhost:27017')
db = client.mytest

addToSet_setOnInsert(db)

