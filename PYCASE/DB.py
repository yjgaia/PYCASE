from PYCASE.BOX import *
from PYCASE import PY_CONFIG
import pymongo
from bson.objectid import ObjectId
import random
import datetime

# https://github.com/UPPERCASE-Series/UPPERCASE.IO/blob/master/SRC/DB/NODE/CONNECT_TO_DB_SERVER.js
def CONNECT_TO_DB_SERVER(name, host='127.0.0.1', port=27017, username=None, password=None):
    client = pymongo.MongoClient(host, port)
    CONNECT_TO_DB_SERVER.nativeDB = client[name]

# https://github.com/UPPERCASE-Series/UPPERCASE.IO/blob/master/SRC/DB/NODE/DB.js
def DB(box):
    class DB(object):

        @staticmethod
        def gen_id(id):
            return ObjectId(id)

        @staticmethod
        def cleanData(data):
            if data.get('_id') is not None:
                data['id'] = str(data['_id'])
                del data['_id']
            if data.get('__IS_ENABLED') is not None:
                del data['__IS_ENABLED']
            if data.get('__RANDOM_KEY') is not None:
                del data['__RANDOM_KEY']
            return data

        @staticmethod
        def removeToDeleteValues(data):
            toDeleteNames = []
            for name, value in data.items():
                if value is None:
                    toDeleteNames.append(name)
                if isinstance(value, dict) or isinstance(value, list):
                    DB.removeToDeleteValues(value)
            for name in toDeleteNames:
                del data[name]

        @staticmethod
        def makeUpFilter(filter, isIncludeRemoved=False):
            def f(filter):
                if filter.get('id') is not None:
                    if isinstance(filter['id'], dict):
                        for i, values in filter['id'].items():
                            if (isinstance(values, dict) is True) or (isinstance(values, list) is True):
                                for j, value in values.items():
                                    values[j] = DB.gen_id(value)
                            else:
                                filter['id'][i] = DB.gen_id(values)
                        filter['_id'] = filter['id']
                    else:
                        filter['_id'] = DB.gen_id(filter['id'])
                    del filter['id']
                if isIncludeRemoved is not True:
                    filter['__IS_ENABLED'] = True
                toDeleteNames = []
                for name, value in filter.items():
                    if value is None:
                        toDeleteNames.append(name)
                for name in toDeleteNames:
                    del filter[name]
            if filter.get('$and') is not None:
                for filter in filter['$and'].values():
                    f(filter)
            elif filter.get('$or') is not None:
                for filter in filter['$or'].values():
                    f(filter)
            else:
                f(filter)

        def __init__(self, name):
            self.collection = CONNECT_TO_DB_SERVER.nativeDB[box.boxName + '.' + name]
            self.historyCollection = CONNECT_TO_DB_SERVER.nativeDB[box.boxName + '.' + name + '__HISTORY']
            self.errorLogCollection = CONNECT_TO_DB_SERVER.nativeDB[box.boxName + '.' + name + '__ERROR']

        def addHistory(self, method, id, change, time):
            savedData = self.historyCollection.find_one({'id': id})
            info = {'method': method, 'change': change, 'time': time}
            if savedData is None:
                self.historyCollection.insert({'id': id, 'timeline': [info]})
            else:
                self.historyCollection.update({'id': id}, {'$push': {'timeline': [info]}})

        def create(self, data):
            data['__IS_ENABLED'] = True  # set is enabled.
            data['__RANDOM_KEY'] = random.random()  # set random key.
            data['createTime'] = datetime.datetime.utcnow()  # set create time.
            DB.removeToDeleteValues(data)
            self.collection.insert(data, True, True)
            savedData = self.collection.find_one({'_id': data['_id']})
            DB.cleanData(savedData)
            self.addHistory('create', savedData['id'], savedData, savedData['createTime'])
            return savedData

        def get(self, filter, sorts=None, isRandom=False, isIncludeRemoved=False):
            if filter is None:
                return None
            if isinstance(filter, str):  # filter is id
                filter = {'_id': DB.gen_id(filter)}
            if sorts is None:
                sorts = [('createTime', -1)]
            sorts.append(('__RANDOM_KEY', 1))
            if isRandom is True:
                randomKey = random.random()
                filter['__RANDOM_KEY'] = {'$gte': randomKey}
                DB.makeUpFilter(filter, isIncludeRemoved)
                savedDataCursor = self.collection.find(filter).sort(sorts).limit(1)
                if savedDataCursor.count() == 0:
                    filter['__RANDOM_KEY'] = {'$lte': randomKey}
                    savedDataCursor = self.collection.find(filter).sort(sorts).limit(1)
            else:
                DB.makeUpFilter(filter, isIncludeRemoved)
                savedDataCursor = self.collection.find(filter).sort(sorts).limit(1)
            if savedDataCursor.count() >= 1:
                savedData = savedDataCursor[0]
                DB.cleanData(savedData)
                return savedData

        def update(self, data):
            id = data['id']
            _unset = None
            _inc = data['$inc']
            filter = {'_id': DB.gen_id(id), '__IS_ENABLED': True}
            isSetData = False
            toDeleteNames = []
            for name, value in data.items():
                if name in ('id', '_id', '__IS_ENABLED', 'createTime', '$inc'):
                    toDeleteNames.append(name)
                elif value is None:
                    if _unset is None:
                        _unset = {}
                    _unset[name] = ''
                    toDeleteNames.append(name)
                else:
                    isSetData = True
            for name in toDeleteNames:
                del data[name]
            data['lastUpdateTime'] = datetime.datetime.utcnow()
            updateData = {'$set': data}
            if _unset is not None:
                updateData['$unset'] = _unset
            if _inc is not None:
                updateData['$inc'] = _inc
            self.collection.update(filter, updateData, False, False, True)
            savedData = self.get(filter)
            if (_inc is None) or (isSetData is True) or (_unset is not None):
                updateData = {}
                if isSetData is True:
                    for name, value in data.items():
                        updateData[name] = value
                if _unset is True:
                    for name, value in _unset.items():
                        updateData[name] = None
                self.addHistory('update', id, updateData, savedData['lastUpdateTime'])
            DB.cleanData(savedData)
            return savedData

        def remove(self, id):
            filter = {'_id': DB.gen_id(id), '__IS_ENABLED': True}
            savedData = self.get(filter)
            removeData = {'__IS_ENABLED': False, 'removeTime': datetime.datetime.utcnow()}
            self.collection.update(filter, {'$set': removeData}, False, False, True)
            self.addHistory('remove', savedData['id'], {'removeTime': removeData['removeTime']}, removeData['removeTime'])
            DB.cleanData(savedData)
            return savedData
        
        def find(self, filter=None, sorts=None, start=0, count=None, isFindAll=False, isIncludeRemoved=False):
            if filter is None:
                filter = {}
            if sorts is None:
                sorts = [('createTime', -1)]
            if isFindAll is not True:
                if (count is None) or (count > PY_CONFIG.maxDataCount) or (isinstance(count, int) is not True):
                    count = PY_CONFIG.maxDataCount
                elif count < 1:
                    count = 1
            DB.makeUpFilter(filter, isIncludeRemoved)
            if isFindAll is True:
                savedDataSet = list(self.collection.find(filter).sort(sorts).skip(start))
            else:
                savedDataSet = list(self.collection.find(filter).sort(sorts).skip(start).limit(count))
            for savedData in savedDataSet:
                DB.cleanData(savedData)
            return savedDataSet
         
        def count(self, filter, isIncludeRemoved=False):
            if isinstance(filter, str):  # filter is id
                filter = {'_id': DB.gen_id(filter)}
            DB.makeUpFilter(filter, isIncludeRemoved)
            return self.collection.find(filter).count()

        def checkIsExists(self, filter, isIncludeRemoved=False):
            if isinstance(filter, str):  # filter is id
                filter = {'_id': DB.gen_id(filter)}
            DB.makeUpFilter(filter, isIncludeRemoved)
            return self.collection.find(filter).count() > 0

    box.DB = DB
FOR_BOX(DB)
