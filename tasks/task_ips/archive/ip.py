import pymongo
from pymongo import MongoClient
import ipaddress

class DBConnector(object):
    def __init__(self):
        client = MongoClient()
        self.db = client.mytest
        
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

class NetworkStatistics(object):
    def __init__(self, network):
        self.network = network
        self.ip_int_min = int(network[0])
        self.ip_int_max = int(network[-1])
        self.times_met = 0

    def __str__(self):
        return "{0:20s}, times met: {1:7d}".format(str(self.network), self.times_met) 

class PrivateIPFinder(object):
    use_wiki = True # use wiki private networks
    
    def __init__(self):        
        if self.use_wiki:
            self.private_networks = [   '0.0.0.0/8',
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
        else:
            self.private_networks = [    '0.0.0.0/8',
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

    def get_filter(self):
        ip_int_filters = []
        for net in self.private_networks:
            net = ipaddress.IPv4Network(net)
            ip_int_filters.append({ 
                                    'ip_int': 
                                    {
                                        '$gte' : int(net[0]), 
                                        '$lte' : int(net[-1]) 
                                    } 
                                   })
            
        return { '$or': ip_int_filters }
    
    def copy_private_ips(self, connector):
        bulk = connector.db.private_ips.initialize_ordered_bulk_op()
        execute_bulk = False
        for doc in connector.db.ip.find(self.get_filter()):
            execute_bulk = True
            bulk.find(
                {
                    '_id': doc['_id']
                }).upsert().update_one(
                {
                    '$set': doc
                })
        return ( bulk.execute()['nUpserted'] if execute_bulk else 0 )
            
    def delete_private_ips(self, connector):
        bulk = connector.db.ip.initialize_ordered_bulk_op()
        bulk.find(self.get_filter()).remove()
        return bulk.execute()['nRemoved']
    
    def private_ip_statistics(self, connector):
        ip_int_filters = []
        network_statistics = []
        for net in self.private_networks:
            net = ipaddress.IPv4Network(net)
            ip_int_filters.append({ 
                                    'ip_int': 
                                    {
                                        '$gte' : int(net[0]), 
                                        '$lte' : int(net[-1]) 
                                    } 
                                   })
            network_statistics.append(NetworkStatistics(net))
        for doc in connector.db.ip.find({ '$or': ip_int_filters }):
            for net in network_statistics:
                # print(0 >= 1 <= 1) # returns False ...
                if (net.ip_int_min <= doc['ip_int'] and 
                    doc['ip_int'] <= net.ip_int_max):
                    net.times_met += 1
                    break
        return network_statistics
    
    def print_statistics(self, connector):
        net_stat = self.private_ip_statistics(connector)
        print('statistics using', ('wiki' if self.use_wiki else 'python'), 'networks')
        total = 0
        for net in net_stat:
            total += net.times_met
            print(net)
        print('total:', total)