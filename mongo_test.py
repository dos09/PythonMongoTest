import pymongo
from pymongo import MongoClient
import ipaddress

ip = '226.236.35.56'
print(ip,'\n  ',int(ipaddress.IPv4Address(ip)))