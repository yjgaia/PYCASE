# https://github.com/UPPERCASE-Series/UPPERCASE.IO/blob/master/SRC/BOX/BOX.js
class BOX(object):
    def __init__(self, boxName):
        self.boxName = boxName
        BOX.boxes[boxName] = self
BOX.boxes = {}

# https://github.com/UPPERCASE-Series/UPPERCASE.IO/blob/master/SRC/BOX/FOR_BOX.js
def FOR_BOX(func):
    for box in BOX.boxes.values():
        func(box)
