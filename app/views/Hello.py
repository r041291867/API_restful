#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint
from flask_restful import Resource, Api

from . import views_blueprint
from app.extensions import restapi,cache
from app.utils import cache_key
import logging
#from app.logging import before_request,after_request

todos = {"todo1":"Remember the milk","todo2":"Change my brakepads"}

@restapi.resource('/Hello')
#@cache.cached(key_prefix='/Hello')
class HelloWorld(Resource):
#	@cache.cached(timeout=600, key_prefix='/Hello', unless=None)
	def get(self):
#		print("Hello No Cache")
		logging.info('Hello is Logging')
		logging.getLogger('error_Logger').error('Hello is Error')
		logging.getLogger('error_Logger').handlers[0].flush()
		return {'hello': 'world'}

@restapi.resource('/Hello/<string:todo_id>')
class TodoSimple(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, todo_id):
		print("todo No Cache")
		return {todo_id: todos[todo_id]}
	def put(self, todo_id):
		todos[todo_id] = request.form['data']
		return {todo_id: todos[todo_id]}
