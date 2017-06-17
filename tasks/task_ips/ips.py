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
    
    def _get_random_ip_address(self):
        r_from, r_to = 0, 255  
        return '{0}.{1}.{2}.{3}'.format(randint(r_from, r_to), randint(r_from, r_to),
                                      randint(r_from, r_to), randint(r_from, r_to))
        
    def generate_data(self):
        col_ip, col_ip_int = 'ip', 'ip_int'        
        for i in range(0, self.count_of_ip_int_strings):
            ip = self._get_random_ip_address()
            yield { col_ip : ip,
                    col_ip_int : str(int(ipaddress.IPv4Address(ip))) }
        for i in range(0, self.count_of_ip_int_integers):
            ip = self._get_random_ip_address()
            yield { col_ip : ip,
                    col_ip_int : int(ipaddress.IPv4Address(ip)) }
        for i in range(0, self.count_of_ip_int_missing):
            yield { col_ip : self._get_random_ip_address() }
                   
class DBConnector(object):
    def __init__(self):
        client = MongoClient()
        self.db = client.mytest
    
    def delete_data(self):
        # return  # for safety ################################################
        print('delete data called')
        """Returns the count of deleted documents"""
        bulk = self.db.ip.initialize_ordered_bulk_op()
        bulk.find({}).remove()
        return bulk.execute()['nRemoved']   
    
    def insert_data(self, data_generator):
        """Returns the count of inserted documents"""
        if(not(isinstance(data_generator, RandomDataGenerator))):
            raise ValueError('data_generator must be an instance'
                             'of RandomDataGenerator')
        
        bulk = self.db.ip.initialize_ordered_bulk_op()
        for doc in data_generator.generate_data():
            bulk.insert(doc)
        return bulk.execute()['nInserted']

    def refresh_ip_collection(self, data_generator):
        self.delete_data()  # if must be used remove the return from first line
        self.insert_data(data_generator)
    
    def fix_data(self):
        """Returns the count of fixed documents or -1 if no bad data exists"""
        bulk = self.db.ip.initialize_ordered_bulk_op()
        has_bad_data = False
        for doc in self.db.ip.find(
            {
                '$or':
                [
                    { 'ip_int': { '$type': 2 } }, # 'string'
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
        return (bulk.execute()['nModified'] if has_bad_data else 0)
    
    def get_bad_data_count(self):
        return self.db.ip.find(
            { 
                '$or':
                [
                    { 'ip_int': { '$type': 2 } }, # 'string'
                    { 'ip_int': { '$exists': False } }
                ] 
            }).count() 
            
class PrivateIPFinder(object):
    def __init__(self):
        self.python_private_networks = [    '0.0.0.0/8',
                                            '10.0.0.0/8',
                                            '127.0.0.0/8',
                                            '169.254.0.0/16',
                                            '172.16.0.0/12',
                                            '192.0.0.0/29',
                                            '192.0.0.170/31',
                                            '192.0.2.0/24',
                                            '192.168.0.0/16',
                                            '198.18.0.0/15',
                                            '198.51.100.0/24',
                                            '203.0.113.0/24',
                                            '240.0.0.0/4',
                                            '255.255.255.255/32']
        self.wiki_private_networks = [  '0.0.0.0/8',
                                        '10.0.0.0/8',
                                        '100.64.0.0/10',
                                        '127.0.0.0/8',
                                        '169.254.0.0/16',
                                        '172.16.0.0/12',
                                        '192.0.0.0/24',
                                        '192.0.2.0/24',
                                        '192.88.99.0/24',
                                        '192.168.0.0/16',
                                        '198.18.0.0/15',
                                        '198.51.100.0/24',
                                        '203.0.113.0/24',
                                        '224.0.0.0/4',
                                        '240.0.0.0/4',
                                        '255.255.255.255/32' ]
    
    def _get_filter(self, private_networks):
        ip_int_filters = []
        for net in private_networks:
            net = ipaddress.IPv4Network(net)
            ip_int_filters.append({ 'ip_int': {'$gte' : int(net[0]), '$lte' : int(net[-1]) } })
            
        return { '$or': ip_int_filters }
    
    def get_wiki_filter(self):
        return self._get_filter(self.wiki_private_networks)
    
    def get_python_filter(self):
        return self._get_fitler(self.python_private_networks)
    
    def _get_private_ips_count(self, connector, private_networks):            
        return connector.db.ip.find(self._get_filter(private_networks)).count()
    
    def get_private_ips_count_using_python_networks(self, connector):
        return self._get_private_ips_count(connector, self.python_private_networks)
    
    def get_private_ips_count_using_wiki_networks(self, connector):
        return self._get_private_ips_count(connector, self.wiki_private_networks)
    
    def _private_ip_statistics(self, connector, private_networks):
        ip_int_filters = []
        network_statistics = []
        for net in private_networks:
            net = ipaddress.IPv4Network(net)
            ip_int_filters.append({ 'ip_int': {'$gte' : int(net[0]), '$lte' : int(net[-1]) } })
            network_statistics.append(NetworkStatistics(net))            
        for doc in connector.db.ip.find({ '$or': ip_int_filters }):
            for net in network_statistics:
                # print(0 >= 1 <= 1) # returns False ...
                if net.ip_int_min <= doc['ip_int'] and doc['ip_int'] <= net.ip_int_max:
                    net.times_met += 1
                    break
        return network_statistics
    
    def private_ip_statistics_using_python_networks(self, connector):
        return self._private_ip_statistics(connector, self.python_private_networks)
    
    def private_ip_statistics_using_wiki_networks(self, connector):
        return self._private_ip_statistics(connector, self.wiki_private_networks)
    
    def move_private_ips_using_wiki_networks(self, connector):
        filter = self.get_wiki_filter()
        bulk = connector.db.private_ips.initialize_ordered_bulk_op()
        execute_bulk = False
        for doc in connector.db.ip.find(filter):
            execute_bulk = True
            bulk.find(
                {
                    'ip': doc['ip']
                }).upsert().update_one(
                {
                    '$set': doc
                })
        if execute_bulk:
            result = bulk.execute()
            print('moved %s docs' % (result['nUpserted']))
        else:
            print('nothing to move to private_ips')
        bulk = connector.db.ip.initialize_ordered_bulk_op()
        bulk.find(filter).remove()
        result = bulk.execute()
        print('deleted %s docs' % result['nRemoved'])

class NetworkStatistics(object):
    def __init__(self, network):
        self.network = network
        self.ip_int_min = int(network[0])
        self.ip_int_max = int(network[-1])
        self.times_met = 0

    def __str__(self):
        return "{0:20s}, times met: {1:7d}".format(str(self.network), self.times_met)        

class Tester(object):
    def test_fixing_data_has_bad_data(self):
        data_generator = RandomDataGenerator(count_of_docs=1000)
        connector = DBConnector()
        bad_docs_count = (data_generator.count_of_ip_int_strings + 
                          data_generator.count_of_ip_int_missing)
                
        connector.delete_data()
        assert connector.insert_data(data_generator) == data_generator.count_of_docs
        assert connector.get_bad_data_count() == bad_docs_count
        assert connector.fix_data() == bad_docs_count
        assert connector.get_bad_data_count() == 0
    def test_fixing_data_no_bad_data(self):
        data_generator = RandomDataGenerator(count_of_docs=1000,
                                             ip_int_strings_percent=0,
                                             ip_int_missing_percent=0)
        connector = DBConnector()
        bad_docs_count = (data_generator.count_of_ip_int_strings + 
                          data_generator.count_of_ip_int_missing)
                
        connector.delete_data()
        assert bad_docs_count == 0
        assert connector.insert_data(data_generator) == data_generator.count_of_docs
        assert connector.get_bad_data_count() == bad_docs_count
        assert connector.fix_data() == 0
        assert connector.get_bad_data_count() == 0
    
def print_statistics():
    connector = DBConnector()
    ip_finder = PrivateIPFinder()
    net_stat_python = ip_finder.private_ip_statistics_using_python_networks(connector)
    print('python\'s private networks:')
    for net in net_stat_python:
        print(net)
    print('wiki\'s private networks:')
    net_stat_wiki = ip_finder.private_ip_statistics_using_wiki_networks(connector)
    for net in net_stat_wiki:
        print(net)

def write_python_statistics_in_db():
    print('writing statistics for private ips using python\'s private networks...')
    connector = DBConnector()
    ip_finder = PrivateIPFinder()
    net_stat_python = ip_finder.private_ip_statistics_using_python_networks(connector)
    # update
    bulk = connector.db.python_privates.initialize_ordered_bulk_op()
    for net in net_stat_python:
        bulk.find({'network':str(net.network)}).upsert().update_one(
            {'$set':
                {
                    'network':str(net.network),
                    'ip_int_min':net.ip_int_min,
                    'ip_int_max':net.ip_int_max,
                    'times_met':net.times_met
                } 
            } )
    bulk.execute()
    print('done writing')

def write_wiki_statistics_in_db():
    print('writing statistics for private ips using wiki\'s private networks...')
    connector = DBConnector()
    ip_finder = PrivateIPFinder()
    net_stat_wiki = ip_finder.private_ip_statistics_using_wiki_networks(connector)
    # update
    bulk = connector.db.wiki_privates.initialize_ordered_bulk_op()
    for net in net_stat_wiki:
        bulk.find({'network':str(net.network)}).upsert().update_one(
            {'$set':
                {
                'network':str(net.network),
                'ip_int_min':net.ip_int_min,
                'ip_int_max':net.ip_int_max,
                'times_met':net.times_met
                } 
            } )
    bulk.execute()
    print('done writing')

# Statistics
# print_statistics()
# write_python_statistics_in_db()
# write_wiki_statistics_in_db()    


connector = DBConnector()
data_generator = RandomDataGenerator(count_of_docs=100000,
                                     ip_int_strings_percent=0,
                                     ip_int_missing_percent=0)
ip_finder = PrivateIPFinder()
# generate data and fix ip_int field ##########################################
bad_docs_count = (data_generator.count_of_ip_int_strings + 
                  data_generator.count_of_ip_int_missing)

print('deleting data...')
start_time = time.time()
connector.delete_data()
print('deletion:', time.time() - start_time, 'seconds')

print('inserting data...')
start_time = time.time()
connector.insert_data(data_generator)
print('insertion:', time.time() - start_time, 'seconds')

print('fixing data...')
start_time = time.time()
fixed_docs_count = connector.fix_data()
print('fixing:', time.time() - start_time, 'seconds')

print('bad docs:', bad_docs_count)
print('fixed docs:', fixed_docs_count)
# private ips logic ###########################################################
# print_statistics()
# print('private ips (using python\'s networks):', 
#       ip_finder.get_private_ips_count_using_python_networks(connector))
# print('private ips (using wiki\'s networks):', 
#       ip_finder.get_private_ips_count_using_wiki_networks(connector))
# ip_finder.move_private_ips_using_wiki_networks(connector)
###############################################################################
# print('bad docs in collection:',connector.get_bad_data_count())
