import ipaddress

# ip_str = '2001:4801:7901:0:c5ce:526c:0:1a'
# ip = ipaddress.IPv6Address(ip_str)
# print(int(ip))
# 42541948612144363323541006660294148122

from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017') 
db =  client.mytest

bulk = db.orcs.initialize_unordered_bulk_op()
weps = ['1haxe', '2hsword'] # 'shield'
bulk.find({'name':'Ra'}).upsert().update_one(
    {
        '$addToSet' : { 'weapons' : { '$each' : weps } }
    }
)
bulk.execute()

# db['tralala222'].update_one( {'_id': 123},
#                              { '$set': {'name':'Natasha', 'age':25} }, 
#                              upsert=True)
# print(db['tralala222'].find().count())

# for doc in db.asd.find({}, { 'name': 1 }):
#     print(doc['name'])