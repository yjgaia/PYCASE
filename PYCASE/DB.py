from PYCASE.BOX import *
import pymongo
import random
import datetime

def CONNECT_TO_DB_SERVER(name, host='127.0.0.1', port=27017, username=None, password=None):
    client = pymongo.MongoClient(host, port)
    CONNECT_TO_DB_SERVER.nativeDB = client[name]

def DB(box):
    class DB(object):
        def __init__(self, name):
            self.collection = CONNECT_TO_DB_SERVER.nativeDB[box.boxName + '.' + name]
        def create(self, data):
            data['__IS_ENABLED'] = True  # set is enabled.
            data['__RANDOM_KEY'] = random.random()  # set random key.
            data['createTime'] = datetime.datetime.now()  # set create time.
            self.collection.insert(data)
    box.DB = DB
FOR_BOX(DB)
