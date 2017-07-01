import pymongo
from pymongo import MongoClient

def printCursorData(cursor, info=None):
    if info is not None:
        print(info)
    for doc in cursor:
        print(doc)

db = MongoClient('mongodb://localhost:27017').mytest
# use mongo_scripts -> collection_orcs_inserts

printCursorData(
    db.orcs.find(
        {
            '$and':
            [
                {
                    '$or': 
                    [
                        {'clan' : { '$in': ['Black wolfs', 'Red orcs'] }},
                        {'clan' : 'Pirates'}
                    ]
                },
                {
                    'kills' : { '$gte': 3, '$lte': 5 }
                }
            ]
            
        },
        {
            'clan':1, '_id':0, 'name':1, 'kills':1
        }
    ).sort([('clan', pymongo.ASCENDING)]),
    'Find from clans Black wolfs, Red orcs, Pirates with kills between 3 and 5'
)

cursor = db.orcs.aggregate(
[
    { 
        '$group': { '_id': '$clan', 'orcs_count': { '$sum': 1 } } 
    }
] )
printCursorData(cursor, 'Group by clan:')
# print(list(cursor))

cursor = db.orcs.aggregate(
[
    {
        '$match': { 'clan': { '$in': ['Black wolfs', 'Red orcs'] } }
    },
    {
        '$group': { '_id': '$clan', 'total_kills': { '$sum': '$kills' } }
    },
    {
        '$sort': { 'total_kills': 1 }
    },
#     {
#         '$project': { 'clan':1 }
#     },
    {
        '$limit': 10
    }
] )
printCursorData(cursor, 'Order clans per kills:')

cursor = db.orcs.aggregate(
[
    {
        '$match': { 'kills': { '$gte':10 } }
    },
    {
        '$project': { 'clan':1, 'name':1, 'kills':1, '_id':0 }
    }
] )
printCursorData(cursor, 'Orcs with more than 10 kills')
cursor = db.orcs.aggregate(
[
    {
        '$match': { 'kills': { '$gte':10 } }
    },
    {
        '$count': 'top_killers'
    }
]
)
printCursorData(cursor, 'Count of killers with more than 10 kills')

