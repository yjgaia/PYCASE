import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httpserver
import os
import pymongo
import operator
import tornado.websocket
import json

from datetime import date
from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps
from json import JSONEncoder
from bson.objectid import ObjectId

dirname = os.path.dirname(__file__)

STATIC_PATH = os.path.join(dirname, 'static')

class IndexDotHTMLAwareStaticFileHandler(tornado.web.StaticFileHandler):
    def parse_url_path(self, url_path):
        if not url_path or url_path.endswith('/'):
            url_path += 'index.html'

        return super(IndexDotHTMLAwareStaticFileHandler, self).parse_url_path(url_path)

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, date):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/test", MainHandler),
            (r'/', WebSocketHandler),
        ]
        settings = {
            "static_path": STATIC_PATH,
            "debug": True,
            # "static_url_prefix": "/static/",
            "static_handler_class" : IndexDotHTMLAwareStaticFileHandler
        }
        tornado.web.Application.__init__(self, handlers, **settings)

class ItemModel:
    def get_items(self):
        client = MongoClient()
        db = client.faust
        inven_c = db.Faust.Inventory
        items = []

        for item in inven_c.find():
            print(item)
            # items.append(item)
            # item['_id'] = str(item['_id'])
            # item['createTime'] = item['createTime'].isoformat()
            items.append(item)

        return items

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        print("OK")
        client = MongoClient()
        db = client.faust
        inven_c = db.Faust.Inventory
        items = []

        for item in inven_c.find():
            print(item)
            # items.append(item)
            # item['_id'] = str(item['_id'])
            # item['createTime'] = item['createTime'].isoformat()
            items.append(item)

        response = { 'version': '3.5.6',
                     'last_build':  date.today().isoformat() }
        # self.write({'items': dumps(items)})
        self.write(JSONEncoder().encode(items))

class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        print("WebSocket opened")

    def on_message(self, message):
        model = ItemModel()
        self.write_message(JSONEncoder().encode(model.get_items()))

    def on_close(self):
        print("WebSocket closed")

def main():
    applicaton = Application()
    http_server = tornado.httpserver.HTTPServer(applicaton)
    http_server.listen(9999)

    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
