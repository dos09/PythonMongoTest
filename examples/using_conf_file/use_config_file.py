from pymongo import MongoClient
import click
# from base.xxx_properties import XXXProperties
# just example how to use it actual code is confidential

@click.command()
@click.option('-c', '--config', required=True,
    type=click.Path(dir_okay=False, readable=True,
                    resolve_path=True, exists=True),
    help='config file')

def main(config):    
    # prints the absolute path (because resolve_path is True)
    print('main:', config)
    xxx_props = XXXProperties(config)
    mongo_url = xxx_props['db.m3.url']
    db_name = xxx_props['db.m3.dbname']
    client = MongoClient(mongo_url)
    db = client[db_name]
    print('mongo url:', mongo_url)
    print('db name:', db_name)
    print('count:', db.ip.find({}).count())
    
if __name__ == '__main__':
    main()
    
# how to run:
# 1. must have click installed in your virtual environment
# 2. activate your virtual environment and from the same terminal navigate to
#     the directory with this script
# 3. python script_name.py -c conf.properties
#     Example contents for conf.properties:
#     db.m3.url = mongodb://127.0.0.1:27017
#     db.m3.dbname = test