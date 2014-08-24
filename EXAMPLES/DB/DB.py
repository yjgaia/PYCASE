import sys
sys.path.append('../..')
import itertools

from PYCASE.BOX import *

TestBox = BOX('TestBox')

from PYCASE.DB import *

CONNECT_TO_DB_SERVER('test')

db = TestBox.DB('test')

data = {
       'msg': 'test',
       'count': 0
       }

savedData = db.create(data)

print('Create data successed!', savedData)
print('Get data successed!', db.get(savedData['id']))
print('Check is exists success!', db.checkIsExists(savedData['id']))
print('Get data using filter successed!', db.get({'msg': 'test'}))

for _ in itertools.repeat(None, 5):
    print('Get random data successed!', db.get(None, None, True))

savedData = db.update({
                       'id': savedData['id'],
                       'msg': 'test2',
                       '$inc': {
                                'count': 1
                                }
                       })

print('Update data successed!', savedData)

savedData = db.remove(savedData['id'])

print('Remove data successed!', savedData)
print('Get data again successed!', db.get(savedData['id']))
print('Get data again successed!', db.get(savedData['id'], None, False, True))
print('Find data set success!', db.find({'msg': 'test'}, [('createTime', -1)], 10, 10));
print('Count data set success!', db.count({'msg': 'test'}))
print('Check is exists success!', db.checkIsExists(savedData['id']))
