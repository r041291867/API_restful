#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint,current_app, make_response
from flask_restful import Resource, Api

from . import views_blueprint
from app.extensions import mysql2,restapi,cache
from app.utils import cache_key
from flask import request
import textwrap
import gzip
import logging
import json
import math
from datetime import date,datetime as dt, timedelta


@restapi.resource('/fulearn/V4/UseKind/SearchKeyWord')
class FulearnV4SearchKeyWord(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
		
		result = {"SearchKeyWords":[]}
			
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetSearchKeyWord();")
			rows = cursor.fetchall()
			for row in rows : 
				result["SearchKeyWords"].append({"KeyWord":row["KeyWord"],"SearchCount":row["SearchCount"]})
			
			cursor.close()
			conn.close()
		except Exception as inst:
			logging.getLogger('error_Logger').error('FulearnV4 Query UserKind Err')
			logging.getLogger('error_Logger').error(inst)
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
@restapi.resource('/fulearn/V4/UseKind/UseKind')
class FulearnV4UseKind(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
		
		result = {"UseKinds":[
		{"UseKind":"SpecialArea","Name":"专区","ClickCount":404776446,"EnterCourseCount":751975}
		,{"UseKind":"BookCity","Name":"书城","ClickCount":280332345,"EnterCourseCount":569033}
		,{"UseKind":"HouseKeeper","Name":"管家","ClickCount":164114018,"EnterCourseCount":345219}
		,{"UseKind":"VideoArea","Name":"视区","ClickCount":31690749,"EnterCourseCount":38585}
		,{"UseKind":"BookShelf","Name":"富学书架","ClickCount":2438321,"EnterCourseCount":1578}
		,{"UseKind":"Set","Name":"设置","ClickCount":159,"EnterCourseCount":5}
		
		]}
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
