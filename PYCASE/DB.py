from PYCASE.BOX import *
from PYCASE import PY_CONFIG
import pymongo
from bson.objectid import ObjectId
import random
import datetime

# https://github.com/UPPERCASE-Series/UPPERCASE.IO/blob/master/SRC/DB/NODE/CONNECT_TO_DB_SERVER.js
def CONNECT_TO_DB_SERVER(name, host='127.0.0.1', port=27017, username=None, password=None):
    client = pymongo.MongoClient(host, port)
    CONNECT_TO_DB_SERVER.native_db = client[name]

# https://github.com/UPPERCASE-Series/UPPERCASE.IO/blob/master/SRC/DB/NODE/DB.js
def DB(box):
    class DB(object):

        @staticmethod
        def gen_id(id):
            return ObjectId(id)

        @staticmethod
        def clean_data(data):
            if data.get('_id') is not None:
                data['id'] = str(data['_id'])
                del data['_id']
            if data.get('__IS_ENABLED') is not None:
                del data['__IS_ENABLED']
            if data.get('__RANDOM_KEY') is not None:
                del data['__RANDOM_KEY']
            return data

        @staticmethod
        def remove_to_delete_values(data):
            to_delete_names = []
            for name, value in data.items():
                if value is None:
                    to_delete_names.append(name)
                if isinstance(value, dict) or isinstance(value, list):
                    DB.remove_to_delete_values(value)
            for name in to_delete_names:
                del data[name]

        @staticmethod
        def make_up_filter(filter, is_include_removed=False):
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
                if is_include_removed is not True:
                    filter['__IS_ENABLED'] = True
                to_delete_names = []
                for name, value in filter.items():
                    if value is None:
                        to_delete_names.append(name)
                for name in to_delete_names:
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
            self.collection = CONNECT_TO_DB_SERVER.native_db[box.box_name + '.' + name]
            self.history_collection = CONNECT_TO_DB_SERVER.native_db[box.box_name + '.' + name + '__HISTORY']
            self.error_log_Collection = CONNECT_TO_DB_SERVER.native_db[box.box_name + '.' + name + '__ERROR']

        def add_history(self, method, id, change, time):
            saved_data = self.history_collection.find_one({'id': id})
            info = {'method': method, 'change': change, 'time': time}
            if saved_data is None:
                self.history_collection.insert({'id': id, 'timeline': [info]})
            else:
                self.history_collection.update({'id': id}, {'$push': {'timeline': [info]}})

        def create(self, data):
            data['__IS_ENABLED'] = True  # set is enabled.
            data['__RANDOM_KEY'] = random.random()  # set random key.
            data['createTime'] = datetime.datetime.utcnow()  # set create time.
            DB.remove_to_delete_values(data)
            self.collection.insert(data, True, True)
            saved_data = self.collection.find_one({'_id': data['_id']})
            DB.clean_data(saved_data)
            self.add_history('create', saved_data['id'], saved_data, saved_data['createTime'])
            return saved_data

        def get(self, filter, sorts=None, is_random=False, is_include_removed=False):
            if filter is None:
                return None
            if isinstance(filter, str):  # filter is id
                filter = {'_id': DB.gen_id(filter)}
            if sorts is None:
                sorts = [('createTime', -1)]
            sorts.append(('__RANDOM_KEY', 1))
            if is_random is True:
                random_key = random.random()
                filter['__RANDOM_KEY'] = {'$gte': random_key}
                DB.make_up_filter(filter, is_include_removed)
                saved_data_cursor = self.collection.find(filter).sort(sorts).limit(1)
                if saved_data_cursor.count() == 0:
                    filter['__RANDOM_KEY'] = {'$lte': random_key}
                    saved_data_cursor = self.collection.find(filter).sort(sorts).limit(1)
            else:
                DB.make_up_filter(filter, is_include_removed)
                saved_data_cursor = self.collection.find(filter).sort(sorts).limit(1)
            if saved_data_cursor.count() >= 1:
                saved_data = saved_data_cursor[0]
                DB.clean_data(saved_data)
                return saved_data

        def update(self, data):
            id = data['id']
            _unset = None
            _inc = data['$inc']
            filter = {'_id': DB.gen_id(id), '__IS_ENABLED': True}
            is_set_data = False
            to_delete_names = []
            for name, value in data.items():
                if name in ('id', '_id', '__IS_ENABLED', 'createTime', '$inc'):
                    to_delete_names.append(name)
                elif value is None:
                    if _unset is None:
                        _unset = {}
                    _unset[name] = ''
                    to_delete_names.append(name)
                else:
                    is_set_data = True
            for name in to_delete_names:
                del data[name]
            data['lastUpdateTime'] = datetime.datetime.utcnow()
            update_data = {'$set': data}
            if _unset is not None:
                update_data['$unset'] = _unset
            if _inc is not None:
                update_data['$inc'] = _inc
            self.collection.update(filter, update_data, False, False, True)
            saved_data = self.get(filter)
            if (_inc is None) or (is_set_data is True) or (_unset is not None):
                update_data = {}
                if is_set_data is True:
                    for name, value in data.items():
                        update_data[name] = value
                if _unset is True:
                    for name, value in _unset.items():
                        update_data[name] = None
                self.add_history('update', id, update_data, saved_data['lastUpdateTime'])
            DB.clean_data(saved_data)
            return saved_data

        def remove(self, id):
            filter = {'_id': DB.gen_id(id), '__IS_ENABLED': True}
            saved_data = self.get(filter)
            remove_data = {'__IS_ENABLED': False, 'removeTime': datetime.datetime.utcnow()}
            self.collection.update(filter, {'$set': remove_data}, False, False, True)
            self.add_history('remove', saved_data['id'], {'removeTime': remove_data['removeTime']}, remove_data['removeTime'])
            DB.clean_data(saved_data)
            return saved_data

        def find(self, filter=None, sorts=None, start=0, count=None, is_find_all=False, is_include_removed=False):
            if filter is None:
                filter = {}
            if sorts is None:
                sorts = [('createTime', -1)]
            if is_find_all is not True:
                if (count is None) or (count > PY_CONFIG.max_data_count) or (isinstance(count, int) is not True):
                    count = PY_CONFIG.max_data_count
                elif count < 1:
                    count = 1
            DB.make_up_filter(filter, is_include_removed)
            if is_find_all is True:
                saved_data_set = list(self.collection.find(filter).sort(sorts).skip(start))
            else:
                saved_data_set = list(self.collection.find(filter).sort(sorts).skip(start).limit(count))
            for saved_data in saved_data_set:
                DB.clean_data(saved_data)
            return saved_data_set

        def count(self, filter, is_include_removed=False):
            if isinstance(filter, str):  # filter is id
                filter = {'_id': DB.gen_id(filter)}
            DB.make_up_filter(filter, is_include_removed)
            return self.collection.find(filter).count()

        def check_is_exists(self, filter, is_include_removed=False):
            if isinstance(filter, str):  # filter is id
                filter = {'_id': DB.gen_id(filter)}
            DB.make_up_filter(filter, is_include_removed)
            return self.collection.find(filter).count() > 0

    box.DB = DB
FOR_BOX(DB)
