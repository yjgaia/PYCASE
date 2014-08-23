class BOX(object):
    def __init__(self, boxName):
        self.boxName = boxName
        BOX.boxes[boxName] = self
BOX.boxes = {}

def FOR_BOX(func):
    for box in BOX.boxes.values():
        func(box)
