import sys
sys.path.append('../..')
import itertools

from PYCASE.BOX import *

test_box = BOX('TestBox')

from PYCASE.DB import *

CONNECT_TO_DB_SERVER('test')

db = test_box.DB('test')

data = {
       'msg': 'test',
       'count': 0
       }

saved_data = db.create(data)

print('Create data successed!', saved_data)
print('Get data successed!', db.get(saved_data['id']))
print('Check is exists success!', db.check_is_exists(saved_data['id']))
print('Get data using filter successed!', db.get({'msg': 'test'}))

for _ in itertools.repeat(None, 5):
    print('Get random data successed!', db.get(None, None, True))

saved_data = db.update({
                       'id': saved_data['id'],
                       'msg': 'test2',
                       '$inc': {
                                'count': 1
                                }
                       })

print('Update data successed!', saved_data)

saved_data = db.remove(saved_data['id'])

print('Remove data successed!', saved_data)
print('Get data again successed!', db.get(saved_data['id']))
print('Get data again successed!', db.get(saved_data['id'], None, False, True))
print('Find data set success!', db.find({'msg': 'test'}, [('createTime', -1)], 10, 10));
print('Count data set success!', db.count({'msg': 'test'}))
print('Check is exists success!', db.check_is_exists(saved_data['id']))
