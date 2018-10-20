#!/usr/bin/python
# -*- coding:utf-8 -*-
#from flask_mongoengine import MongoEngine
#db = MongoEngine()
#from flask.ext.pymongo import PyMongo

#from flask_mongoengine import MongoEngine
#from flask.ext import restful
from flaskext.mysql import MySQL as MySQL2
from flask_restful import Api, output_json
from flask_cache import Cache
from flask_pymongo import PyMongo
from pymysql.cursors import DictCursor
from flask_mysqldb import MySQL
from flask_compress import Compress


#db = MongoEngine()
restapi= Api()
#mysql = MySQL(cursorclass=DictCursor)
mysql = MySQL()
mysql2 = MySQL2(cursorclass=DictCursor)
mysql51 = MySQL2(cursorclass=DictCursor)
cache = Cache()
mongo = PyMongo()
mongo_flnet = PyMongo()
mongo_fulearn4 = PyMongo()
compress = Compress()
