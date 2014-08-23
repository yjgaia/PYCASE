import sys
sys.path.append('../..')

from PYCASE.BOX import *

TestBox = BOX('TestBox')

from PYCASE.DB import *

CONNECT_TO_DB_SERVER('test')

db = TestBox.DB('test')

db.create({
           'msg': 'test',
           'count': 0
           })
