import pymongo
from pymongo import MongoClient
from random import randint
import ipaddress
import time
import pytest

class RandomDataGenerator(object):
    def __init__(self, count_of_docs=10,
                 ip_int_strings_percent=0.75, ip_int_missing_percent=0.1):
        """How many documents to generate,
        the percent of ip_int fields with string data type,
        the percent of ip_int fields that will be missing,
        the rest ip_int fields will be of type int"""
        # ip_int_strings_percent = 1
        # 0 >= ip_int_strings_percent <= 1 # this returns False
        if(count_of_docs <= 0):
            raise ValueError('Invalid count_of_docs')
        if(ip_int_strings_percent < 0 or ip_int_strings_percent > 1):
            raise ValueError('Invalid percent for ip_int_strings_percent')
        if(ip_int_missing_percent < 0 or ip_int_missing_percent > 1):
            raise ValueError('Invalid percent for ip_int_missing_percent')
        if(ip_int_strings_percent + ip_int_missing_percent > 1):
            raise ValueError('Percent for strings + '
                             'percent for missing fields should be <= 1')
        self.count_of_docs = int(count_of_docs)
        self.count_of_ip_int_strings = int(self.count_of_docs * ip_int_strings_percent)
        self.count_of_ip_int_missing = int(self.count_of_docs * ip_int_missing_percent)
        self.count_of_ip_int_integers = (self.count_of_docs - 
        (self.count_of_ip_int_strings + self.count_of_ip_int_missing))
#         print('self.count_of_docs',self.count_of_docs,
#               'self.count_of_ip_int_strings',self.count_of_ip_int_strings,
#               'self.count_of_ip_int_missing',self.count_of_ip_int_missing,
#               'self.count_of_ip_int_integers',self.count_of_ip_int_integers)
    
    def __get_random_ip_address(self):
        r_from, r_to = 0, 255  
        return '{0}.{1}.{2}.{3}'.format(randint(r_from, r_to), randint(r_from, r_to),
                                      randint(r_from, r_to), randint(r_from, r_to))
        
    def generate_data(self):
        col_ip, col_ip_int = 'ip', 'ip_int'        
        for i in range(0, self.count_of_ip_int_strings):
            ip = self.__get_random_ip_address()
            yield { col_ip : ip,
                    col_ip_int : str(int(ipaddress.IPv4Address(ip))) }
        for i in range(0, self.count_of_ip_int_integers):
            ip = self.__get_random_ip_address()
            yield { col_ip : ip,
                    col_ip_int : int(ipaddress.IPv4Address(ip)) }
        for i in range(0, self.count_of_ip_int_missing):
            yield { col_ip : self.__get_random_ip_address() }
                   
class DBConnector(object):
    def __init__(self):
        client = MongoClient()
        self.db = client.mytest
    
    def delete_data(self):
        # return  # for safety ###################################################
        print('delete data called')
        """Returns the count of deleted documents"""
        bulk = self.db.ip.initialize_ordered_bulk_op()
        bulk.find({}).remove()
        return bulk.execute()['nRemoved']   
    
    def insert_data(self, data_generator):
        """Returns the count of inserted documents"""
        if(not(isinstance(data_generator, RandomDataGenerator))):
            raise ValueError('data_generator must be an instance of RandomDataGenerator')
        
        bulk = self.db.ip.initialize_ordered_bulk_op()
        for doc in data_generator.generate_data():
            bulk.insert(doc)
        return bulk.execute()['nInserted']

    def refresh_ip_collection(self, data_generator):
        self.delete_data()  # if must be used remove the return from the beginning
        self.insert_data(data_generator)
    
    def fix_data(self):
        """Returns the count of fixed documents or -1 if no bad data exists"""
        bulk = self.db.ip.initialize_ordered_bulk_op()
        has_bad_data = False
        for doc in self.db.ip.find(
            { 
                '$or':
                [
                    { 'ip_int': { '$type': 'string' } },
                    { 'ip_int': { '$exists': False } }
                ] 
            }):
            has_bad_data = True
            bulk.find(
                { 
                    '_id': doc['_id'] 
                }).update_one(
                { 
                    '$set': { 'ip_int': int(ipaddress.IPv4Address(doc['ip'])) } 
                })
        return (bulk.execute()['nModified'] if has_bad_data else -1)
    def get_bad_data_count(self):
        return self.db.ip.find(
            { 
                '$or':
                [
                    { 'ip_int': { '$type': 'string' } },
                    { 'ip_int': { '$exists': False } }
                ] 
            }).count()
        
class Tester(object):
    def test(self):
        data_generator = RandomDataGenerator(count_of_docs=1000)
        connector = DBConnector()
        bad_docs_count = (data_generator.count_of_ip_int_strings + 
                          data_generator.count_of_ip_int_missing)
                
        connector.delete_data()
        assert connector.insert_data(data_generator) == data_generator.count_of_docs
        assert connector.get_bad_data_count() == bad_docs_count
        assert connector.fix_data() == bad_docs_count
        assert connector.get_bad_data_count() == 0

data_generator = RandomDataGenerator(count_of_docs=1000)
connector = DBConnector()
bad_docs_count = (data_generator.count_of_ip_int_strings + 
                  data_generator.count_of_ip_int_missing)

connector.delete_data()
connector.insert_data(data_generator)
start_time = time.time()
fixed_docs_count = connector.fix_data()
end_time = time.time()        
print('bad docs:', bad_docs_count)
print('fixed docs:', fixed_docs_count)
print('fix time:', end_time - start_time, 'seconds')

