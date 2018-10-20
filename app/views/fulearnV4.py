#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint,current_app, make_response
from flask_restful import Resource, Api

from . import views_blueprint
from app.extensions import mongo,mysql2,restapi,cache
from app.utils import cache_key
from flask import request
import textwrap
import gzip
import logging
import json
import math

@restapi.resource('/fulearn/V4')
class fulearnV4(Resource):
	def get(self):
		return {'hello': 'fulearnV4-api'}
		
#@restapi.resource('/fulearn/V4/data2')
#class TestPostData(Resource):
#	def get(self):
#		return {'hello': 'fulearnV4-api'}
#		
#	def post(self):
#		print(request.data)
#		return request.data

