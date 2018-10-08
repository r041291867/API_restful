#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint, json, make_response
from flask_restful import Resource, Api, reqparse

from . import views_blueprint
from app.extensions import restapi,cache,mysql,mysql2
from app.utils import cache_key
from MySQLdb import cursors
import datetime
import logging
import decimal
import os
import codecs

@restapi.resource('/fulearn/OldAPI')
class fulearnOldAPI(Resource):
	def get(self):
		return {'hello':'fulearnOldAPI-api'}
		
@restapi.resource('/fulearn/OldAPI/<string:fun>')
class getOldAPI(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self,fun):
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_OldAPI(%s,%s)',[fun,None])
		values = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in values:
			for v in row.keys():
				if type(row[v]) == datetime.date or type(row[v]) == datetime.timedelta:
					#row[v]=datetime.datetime.strptime(str(row[v]),'%Y-%m-%d')
					row[v]=str(row[v])
				if type(row[v]) == decimal.Decimal:
					row[v]=float(row[v])
		return make_response(json.dumps(values,ensure_ascii=False))

@restapi.resource('/fulearn/OldAPI/<string:fun>/<string:val>')
class getOldAPIwithVal(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self,fun,val):
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.callproc('fulearn_2_view.SP_OldAPI',(fun,val))
		values = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in values:
			for v in row.keys():
				if type(row[v]) == datetime.date or type(row[v]) == datetime.timedelta:
					#row[v]=datetime.datetime.strptime(str(row[v]),'%Y-%m-%d')
					row[v]=str(row[v])
				if type(row[v]) == decimal.Decimal:
					row[v]=float(row[v])
		return make_response(json.dumps(values,ensure_ascii=False))
