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


@restapi.resource('/fulearn/V4/Class/<string:ClassName>/TotalCourse')
class FulearnV4ClassTotalCourse(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, ClassName, headers=None):
		
		result = {"TotalCourse":0}
			
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetTotalCoursesByClass(%s);",ClassName)
			rows = cursor.fetchall()
			result["TotalCourse"] = rows[0]["TotalCourse"]
			
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
@restapi.resource('/fulearn/V4/Class/<string:ClassName>/TotalChapter')
class FulearnV4ClassTotalChapter(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, ClassName, headers=None):
		
		result = {"TotalChapter":0}
			
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetTotalCoursesByChapter(%s);",ClassName)
			rows = cursor.fetchall()
			result["TotalChapter"] = rows[0]["TotalChapter"]
			
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
# @restapi.resource('/fulearn/V4/Class/<string:ClassName>/Ranking')
# class FulearnV4ClassRanking(Resource):
	# @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	# def get(self, ClassName, headers=None):
		
		# result = {"Ranking":[
			# {"CourseId":"10000063","Name":"品质意识","Popular":5.0,"Collection":1000,"PlayCoverRatio":191}
			# ,{"CourseId":"10000076","Name":"员工激励","Popular":5.0,"Collection":1000,"PlayCoverRatio":191}
			# ,{"CourseId":"10000065","Name":"现场管理实务之一","Popular":5.0,"Collection":1000,"PlayCoverRatio":191}
			# ,{"CourseId":"10000056","Name":"工作职责","Popular":5.0,"Collection":1000,"PlayCoverRatio":191}
			# ,{"CourseId":"10000033","Name":"人际沟通","Popular":5.0,"Collection":1000,"PlayCoverRatio":191}
			# ,{"CourseId":"10000038","Name":"无尘室管理规定","Popular":5.0,"Collection":1000,"PlayCoverRatio":191}
			# ,{"CourseId":"10000069","Name":"人资政策及SER之二","Popular":5.0,"Collection":1000,"PlayCoverRatio":191}
			# ,{"CourseId":"10000266","Name":"工会关爱","Popular":5.0,"Collection":1000,"PlayCoverRatio":191}
			# ,{"CourseId":"10000070","Name":"人资政策及SER之三","Popular":5.0,"Collection":1000,"PlayCoverRatio":191}
			# ,{"CourseId":"10000077","Name":"冲突管理","Popular":5.0,"Collection":1000,"PlayCoverRatio":191}
		# ]}
		# resp = make_response(
				# json.dumps(result ,
				# ensure_ascii=False))
		# resp.headers.extend(headers or {})
		# return resp
# @restapi.resource('/fulearn/V4/Class/<string:ClassName>/ProvideGroups')
# class FulearnV4ClassProvideGroups(Resource):
	# @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	# def get(self, ClassName, headers=None):
		
		# result = {"ProvideGroups":[
			# {"Class":"管理知识","TotalChapter":1023,"Provides":[{"Group":"IE技委會","TotalChapter":123},{"Group":"人資技委會","TotalChapter":900}]}
			# ,{"Class":"通用技能","TotalChapter":771,"Provides":[{"Group":"IE技委會","TotalChapter":1},{"Group":"人資技委會","TotalChapter":770}]}
			# ,{"Class":"农民工转型","TotalChapter":124,"Provides":[{"Group":"IE技委會","TotalChapter":80},{"Group":"人資技委會","TotalChapter":44}]}
			# ,{"Class":"百科知识","TotalChapter":2034,"Provides":[{"Group":"IE技委會","TotalChapter":12},{"Group":"人資技委會","TotalChapter":2020}]}
			# ,{"Class":"专业知识","TotalChapter":903,"Provides":[{"Group":"IE技委會","TotalChapter":567},{"Group":"人資技委會","TotalChapter":336}]}
		# ]}
		# resp = make_response(
				# json.dumps(result ,
				# ensure_ascii=False))
		# resp.headers.extend(headers or {})
		# return resp
# @restapi.resource('/fulearn/V4/Class/<string:ClassName>/Teachers')
# class FulearnV4ClassTeachers(Resource):
	# @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	# def get(self, ClassName, headers=None):
		
		# result = {"Teachers":[
			# {"Class":"管理知识","TotalChapter":1023,"Provides":[{"Teacher":"IE技委會","TotalChapter":123},{"Teacher":"人資技委會","TotalChapter":900}]}
			# ,{"Class":"通用技能","TotalChapter":771,"Provides":[{"Teacher":"IE技委會","TotalChapter":1},{"Teacher":"人資技委會","TotalChapter":770}]}
			# ,{"Class":"农民工转型","TotalChapter":124,"Provides":[{"Teacher":"IE技委會","TotalChapter":80},{"Teacher":"人資技委會","TotalChapter":44}]}
			# ,{"Class":"百科知识","TotalChapter":2034,"Provides":[{"Teacher":"IE技委會","TotalChapter":12},{"Teacher":"人資技委會","TotalChapter":2020}]}
			# ,{"Class":"专业知识","TotalChapter":903,"Provides":[{"Teacher":"IE技委會","TotalChapter":567},{"Teacher":"人資技委會","TotalChapter":336}]}
		# ]}
		# resp = make_response(
				# json.dumps(result ,
				# ensure_ascii=False))
		# resp.headers.extend(headers or {})
		# return resp