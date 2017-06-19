from random import randint
import ipaddress
import time
from ip import PrivateIPFinder
import pytest


class RandomDataGenerator:

    def __init__(self, count_of_docs=10,
                 ip_int_strings_percent=0.75, ip_int_missing_percent=0.1):
        """How many documents to generate,
        the percent of ip_int fields with string data type,
        the percent of ip_int fields that will be missing,
        the rest ip_int fields will be of type int"""
        # ip_int_strings_percent = 1
        # 0 >= ip_int_strings_percent <= 1 # this returns False
        if (count_of_docs <= 0):
            raise ValueError('Invalid count_of_docs')
        if (ip_int_strings_percent < 0 or ip_int_strings_percent > 1):
            raise ValueError('Invalid percent for ip_int_strings_percent')
        if (ip_int_missing_percent < 0 or ip_int_missing_percent > 1):
            raise ValueError('Invalid percent for ip_int_missing_percent')
        if (ip_int_strings_percent + ip_int_missing_percent > 1):
            raise ValueError('Percent for strings + '
                             'percent for missing fields should be <= 1')
        self.count_of_docs = int(count_of_docs)
        self.count_of_ip_int_strings = int(self.count_of_docs *
                                           ip_int_strings_percent)
        self.count_of_ip_int_missing = int(self.count_of_docs *
                                           ip_int_missing_percent)
        self.count_of_ip_int_integers = (self.count_of_docs -
        (self.count_of_ip_int_strings + self.count_of_ip_int_missing))
#         print('self.count_of_docs',self.count_of_docs,
#               'self.count_of_ip_int_strings',self.count_of_ip_int_strings,
#               'self.count_of_ip_int_missing',self.count_of_ip_int_missing,
#               'self.count_of_ip_int_integers',self.count_of_ip_int_integers)

    @staticmethod
    def _get_random_ip_address():
        r_from, r_to = 0, 255
        return '{0}.{1}.{2}.{3}'.format(randint(r_from, r_to),
                                        randint(r_from, r_to),
                                        randint(r_from, r_to),
                                        randint(r_from, r_to))

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

class DBConnectorTester:

    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.db = db_connector.db

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

    def get_bad_data_count(self):
        return self.db.ip.find(
            {
                '$or':
                [
                    { 'ip_int': { '$type': 2 } },  # 'string'
                    { 'ip_int': { '$exists': False } }
                ]
            }).count()


class Tester:
    
    @pytest.mark.skip
    def test_insert_data_no_bad_data(self, db_connector):
        data_generator = RandomDataGenerator(count_of_docs=1000, 
                                             ip_int_strings_percent=0, 
                                             ip_int_missing_percent=0)
        con_tester = db_connector        
        
        assert (data_generator.count_of_ip_int_strings +
                data_generator.count_of_ip_int_missing) == 0
        con_tester.delete_data()
        assert con_tester.insert_data(data_generator) == data_generator.count_of_docs
        
    def test_insert_data_has_bad_data(self, db_connector):
        data_generator = RandomDataGenerator(count_of_docs=1000, 
                                             ip_int_strings_percent=0.7, 
                                             ip_int_missing_percent=0.2)
        con_tester = db_connector        
        
        con_tester.delete_data()
        assert con_tester.insert_data(data_generator) == data_generator.count_of_docs
    
    @pytest.mark.skip
    def test_fixing_data_has_bad_data(self, db_connector):
        data_generator = RandomDataGenerator(count_of_docs=1000)
        con_tester = db_connector
        bad_docs_count = (data_generator.count_of_ip_int_strings +
                          data_generator.count_of_ip_int_missing)

        con_tester.delete_data()
        assert con_tester.insert_data(data_generator) == data_generator.count_of_docs
        assert con_tester.get_bad_data_count() == bad_docs_count
        assert con_tester.db_connector.fix_data() == bad_docs_count
        assert con_tester.get_bad_data_count() == 0
    
    @pytest.mark.skip
    def test_fixing_data_no_bad_data(self, db_connector):
        data_generator = RandomDataGenerator(count_of_docs=1000,
                                             ip_int_strings_percent=0,
                                             ip_int_missing_percent=0)
        con_tester = db_connector
        bad_docs_count = (data_generator.count_of_ip_int_strings +
                          data_generator.count_of_ip_int_missing)

        con_tester.delete_data()
        assert bad_docs_count == 0
        assert con_tester.insert_data(data_generator) == data_generator.count_of_docs
        assert con_tester.get_bad_data_count() == bad_docs_count
        assert con_tester.db_connector.fix_data() == 0
        assert con_tester.get_bad_data_count() == 0
    
    @pytest.mark.skip
    def test_private_ips(self, db_connector):
        con_tester =  db_connector
        data_generator = RandomDataGenerator(count_of_docs=1000,
                                             ip_int_strings_percent=0,
                                             ip_int_missing_percent=0)
        ip_finder = PrivateIPFinder()

        con_tester.delete_data()
        con_tester.insert_data(data_generator)
        assert (data_generator.count_of_ip_int_strings +
                data_generator.count_of_ip_int_missing) == 0
        assert (ip_finder.copy_private_ips(con_tester.db_connector) ==
                ip_finder.delete_private_ips(con_tester.db_connector))


@pytest.mark.skip
def test_manual_run(db_connector):
    con_tester = db_connector
    data_generator = RandomDataGenerator(count_of_docs=1000,
                                         ip_int_strings_percent=0.5,
                                         ip_int_missing_percent=0.2)
    ip_finder = PrivateIPFinder()
    # generate data and fix ip_int field ######################################
    print('deleting data...')
    start_time = time.time()
    con_tester.delete_data()
    print('deletion:', time.time() - start_time, 'seconds')

    print('inserting data...')
    start_time = time.time()
    con_tester.insert_data(data_generator)
    print('insertion:', time.time() - start_time, 'seconds')

    print('fixing data...')
    start_time = time.time()
    fixed_docs_count = con_tester.db_connector.fix_data()
    print('fixing:', time.time() - start_time, 'seconds')
    print('bad docs:', (data_generator.count_of_ip_int_strings +
                        data_generator.count_of_ip_int_missing))
    print('fixed docs:', fixed_docs_count)
    # private ips logic #######################################################
    ip_finder.print_statistics(con_tester.db_connector)
    print('private ip-s copied to other collection:',
          ip_finder.copy_private_ips(con_tester.db_connector))
    print('private ip-s deleted from ip collection:',
          ip_finder.delete_private_ips(con_tester.db_connector))
