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


# @restapi.resource('/fulearn/V4/Group/<string:GroupName>/TotalUser/')
# class FulearnV4GroupTotalUser(Resource):
	# @cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	# def get(self, GroupName):
		# result = {'TotalUser' : 0}
		# try :
			# conn = mysql2.connect()
			# cursor = conn.cursor()
			# cursor.execute("call fulearn_4_view.SP_GetGroupTotalUser(%s);",GroupName)
			# rows = cursor.fetchall()
			# result['TotalUser'] = int(rows[0]['TotalUser'])
			# cursor.close()
			# conn.close()
		# except Exception as inst:
			# logging.getLogger('error_Logger').error('FulearnV4 Query TotalUser Err')
			# logging.getLogger('error_Logger').error(inst)
		# return result
# @restapi.resource('/fulearn/V4/Group/<string:GroupName>/<string:UserKind>/<YearMonthQuarterly:YearMonthQuarterly>')
# class FulearnV4GroupMonthUserKind(Resource):
	# @cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	# def get(self, GroupName,UserKind,YearMonthQuarterly):
		
		# result = {}
		# QueryKind = ''
		# if UserKind == 'NewUser' :
			# result['NewUser'] = 0
			# QueryKind = 'NewUser'
		# elif UserKind == 'ActiveUser' :
			# result['ActiveUser'] = 0
			# QueryKind = 'Active'
		# elif UserKind == 'NewActiveUser' :
			# result['NewActiveUser'] = 0
			# QueryKind = 'Active'
		# elif UserKind == 'UnactiveUser' :
			# result['UnactiveUser'] = 0
			# QueryKind = 'Unactive'
		# elif UserKind == 'NewUnactiveUser' :
			# result['NewUnactiveUser'] = 0
			# QueryKind = 'Unactive'
		# elif UserKind == 'Dormancy' :
			# result['Dormancy'] = 0
			# QueryKind = 'Dormancy'
		# elif UserKind == 'LoyarUser' :
			# result['LoyarUser'] = 0
			# QueryKind = 'Active'
		# elif UserKind == 'NewLoyarUser' :
			# result['NewLoyarUser'] = 0
			# QueryKind = 'Active'
		# elif UserKind == 'ReturnUser' :
			# result['ReturnUser'] = 0
			# QueryKind = 'Active'
		# elif UserKind == 'LostUser' :
			# result['LostUser'] = 0
			# QueryKind = 'Dormancy'
		# elif UserKind == 'TotalUser' :
			# result['TotalUser'] = 0
			# QueryKind = 'Total'
			
		
		# try :
			# conn = mysql2.connect()
			# cursor = conn.cursor()
			# cursor.execute("call fulearn_4_view.SP_GetUserCountByGroupAndDate(%s,%s,%s,%s);"
				# ,[QueryKind
				# ,GroupName
				# ,YearMonthQuarterly['Start'].strftime('%Y-%m-%d')
				# ,YearMonthQuarterly['End'].strftime('%Y-%m-%d')])
			# rows = cursor.fetchall()
			# if UserKind == 'NewUser' :
				# result['NewUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'ActiveUser':
				# result['ActiveUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'NewActiveUser':
				# result['NewActiveUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'UnactiveUser' :
				# result['UnactiveUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'NewUnactiveUser' :
				# result['NewUnactiveUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'Dormancy' :
				# result['Dormancy'] = int(rows[0]['UserCount'])
			# elif UserKind == 'LoyarUser' :
				# result['LoyarUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'NewLoyarUser' :
				# result['NewLoyarUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'ReturnUser' :
				# result['ReturnUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'LostUser' :
				# result['LostUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'TotalUser' :
				# result['TotalUser'] = int(rows[0]['UserCount'])
			
			# cursor.close()
			# conn.close()
		# except Exception as inst:
			# logging.getLogger('error_Logger').error('FulearnV4 Query UserKind Month Err')
			# logging.getLogger('error_Logger').error(inst)
		# return result
		
# @restapi.resource('/fulearn/V4/Group/<string:GroupName>/<string:UserKind>')
# class FulearnV4GroupUserKind(Resource):
	# @cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	# def get(self, GroupName):
		
		# result = {}
		# QueryKind = ''
		# if UserKind == 'NewUser' :
			# result['NewUser'] = 0
			# QueryKind = 'NewUser'
		# elif UserKind == 'ActiveUser' :
			# result['ActiveUser'] = 0
			# QueryKind = 'Active'
		# elif UserKind == 'NewActiveUser' :
			# result['NewActiveUser'] = 0
			# QueryKind = 'Active'
		# elif UserKind == 'UnactiveUser' :
			# result['UnactiveUser'] = 0
			# QueryKind = 'Unactive'
		# elif UserKind == 'NewUnactiveUser' :
			# result['NewUnactiveUser'] = 0
			# QueryKind = 'Unactive'
		# elif UserKind == 'Dormancy' :
			# result['Dormancy'] = 0
			# QueryKind = 'Dormancy'
		# elif UserKind == 'LoyarUser' :
			# result['LoyarUser'] = 0
			# QueryKind = 'Active'
		# elif UserKind == 'NewLoyarUser' :
			# result['NewLoyarUser'] = 0
			# QueryKind = 'Active'
		# elif UserKind == 'ReturnUser' :
			# result['ReturnUser'] = 0
			# QueryKind = 'Active'
		# elif UserKind == 'LostUser' :
			# result['LostUser'] = 0
			# QueryKind = 'Active'
			
		# try :
			# conn = mysql2.connect()
			# cursor = conn.cursor()
			# cursor.execute("call fulearn_4_view.SP_GetUserCountByGroupAndDate(%s,%s,null,null);",[QueryKind,GroupName])
			# rows = cursor.fetchall()
			
			# if UserKind == 'NewUser' :
				# result['NewUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'ActiveUser':
				# result['ActiveUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'UnactiveUser' :
				# result['UnactiveUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'Dormancy' :
				# result['Dormancy'] = int(rows[0]['UserCount'])
			# elif UserKind == 'LoyarUser' :
				# result['LoyarUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'ReturnUser' :
				# result['ReturnUser'] = int(rows[0]['UserCount'])
			# elif UserKind == 'LostUser' :
				# result['LostUser'] = int(rows[0]['UserCount'])
				
			# cursor.close()
			# conn.close()
		# except Exception as inst:
			# logging.getLogger('error_Logger').error('FulearnV4 Query UserKind Err')
			# logging.getLogger('error_Logger').error(inst)
		# return result

# @restapi.resource('/fulearn/V4/Group/<string:GroupName>/<string:UserKind>/<DateStartHourToEndHour:DateStartHourToEndHour>')
# class FulearnV4FullDayOnlineUser(Resource):
	# @cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	# def get(self,GroupName,UserKind,DateStartHourToEndHour):
		# result = {"Hours":[
		# ]}
		# QueryKind = 'Online'
		# if UserKind == 'OnlineUser' :
			# QueryKind = 'Online'
		# elif UserKind == 'LoginUser' :
			# QueryKind = 'Login'
		# elif UserKind == 'LogoutUser' :
			# QueryKind = 'Logout'
		# Hour = int(DateStartHourToEndHour['StartHour'])
		# EndHour = int(DateStartHourToEndHour['EndHour'])
		# try :
			# conn = mysql2.connect()
			# cursor = conn.cursor()
			# cursor.execute("call fulearn_4_view.SP_GetHourLoginoutUserCountByDate(%s,%s,%s,%s);"
				# ,[QueryKind
				# ,dt.strftime(DateStartHourToEndHour['Date'],'%Y-%m-%d')
				# ,str(DateStartHourToEndHour['StartHour'])
				# ,str(DateStartHourToEndHour['EndHour'])
				# ])
			# rows = cursor.fetchall()
			# for row in rows :
				# if Hour < row["Hour"] :
					# while Hour < row["Hour"] :
						# result["Hours"].append({"Hour":Hour,QueryKind:row["UserCount"]})
						# Hour += 1
				# result["Hours"].append({"Hour":row["Hour"],QueryKind:row["UserCount"]})
				# Hour += 1
			# while Hour <= EndHour :
				# result["Hours"].append({"Hour":Hour,QueryKind:row["UserCount"]})
				# Hour += 1
			
			# cursor.close()
			# conn.close()
		# except Exception as inst:
			# logging.getLogger('error_Logger').error('FulearnV4 Query UserKind Err')
			# logging.getLogger('error_Logger').error(inst)
		# return result

@restapi.resource('/fulearn/V4/Group/<string:GroupName>/Ranking')
class FulearnV4GroupRankingUser(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self, GroupName, headers=None):
		
		result = {"Ranking":[
		{"UserId":"F1218508","Name":"趙小輝","LearnSumSec":315228,"PassCourseCount":304,"CourseCount":380,"Reach":80}
		,{"UserId":"F1218829","Name":"王麗華","LearnSumSec":553,"PassCourseCount":10,"CourseCount":13,"Reach":75}
		,{"UserId":"F7680723","Name":"趙天浪","LearnSumSec":5783,"PassCourseCount":18,"CourseCount":26,"Reach":70}
		,{"UserId":"F7681941","Name":"黃淼","LearnSumSec":113741,"PassCourseCount":383,"CourseCount":589,"Reach":65}
		,{"UserId":"F7682779","Name":"郭曉峰","LearnSumSec":18703,"PassCourseCount":70,"CourseCount":117,"Reach":60}
		,{"UserId":"F1219791","Name":"黃祥述","LearnSumSec":4061,"PassCourseCount":9,"CourseCount":17,"Reach":55}
		,{"UserId":"F1205806","Name":"易廣州","LearnSumSec":113654,"PassCourseCount":205,"CourseCount":410,"Reach":50}
		,{"UserId":"F6300567","Name":"蘇長磊","LearnSumSec":182,"PassCourseCount":2,"CourseCount":4,"Reach":45}
		,{"UserId":"F7683314","Name":"朱佳","LearnSumSec":865,"PassCourseCount":1,"CourseCount":2,"Reach":40}
		,{"UserId":"F1218175","Name":"張玉強","LearnSumSec":58426,"PassCourseCount":37,"CourseCount":106,"Reach":35}
		]}
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
@restapi.resource('/fulearn/V4/Group/<string:GroupName>/Edu')
class FulearnV4GroupEduRatio(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self, GroupName, headers=None):
		
		result = {"Users":[]}
			
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetGroupEduRatio(%s);",GroupName)
			rows = cursor.fetchall()
			for row in rows :
				result["Users"].append({
					"Edu":row["Edu"]
					,"UserCount":row["UserCount"]
					,"Ratio":row["Percent"]
				})
			
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
@restapi.resource('/fulearn/V4/Group/<string:GroupName>/Sex')
class FulearnV4GroupSexRatio(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self, GroupName, headers=None):
		
		result = {"Users":[]}
			
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetGroupSexRatio(%s);",GroupName)
			rows = cursor.fetchall()
			for row in rows :
				result["Users"].append({
					"Sex":row["Sex"]
					,"UserCount":row["UserCount"]
				})
			
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
		
@restapi.resource('/fulearn/V4/Group/<string:GroupName>/Manage')
class FulearnV4GroupManageRatio(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self, GroupName, headers=None):
		
		result = {"Users":[]}
			
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetGroupManageRatio(%s);",GroupName)
			rows = cursor.fetchall()
			for row in rows :
				result["Users"].append({
					"IsManage":row["IsManage"]
					,"UserCount":row["UserCount"]
				})
			
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
		
@restapi.resource('/fulearn/V4/Group/<string:GroupName>/Salary')
class FulearnV4GroupSalaryRatio(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self, GroupName, headers=None):
		
		result = {"Users":[]}
			
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetGroupSalaryRatio(%s);",GroupName)
			rows = cursor.fetchall()
			for row in rows :
				result["Users"].append({
					"Salary":row["Salary"]
					,"UserCount":row["UserCount"]
				})
			
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
		
@restapi.resource('/fulearn/V4/Group/<string:GroupName>/Position')
class FulearnV4GroupPositionRatio(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self, GroupName, headers=None):
		
		result = {"Users":[]}
			
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetGroupPositionRatio(%s);",GroupName)
			rows = cursor.fetchall()
			for row in rows :
				result["Users"].append({
					"Position":row["Position"]
					,"UserCount":row["UserCount"]
				})
			
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
@restapi.resource('/fulearn/V4/Group/<string:GroupName>/Courses')
class FulearnV4GroupLearnCourses(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self, GroupName, headers=None):
		
		result = {"Courses":[]}
			
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetCourseLearnInfoByGroup(%s);",GroupName)
			rows = cursor.fetchall()
			for row in rows :
				result["Courses"].append({
					"CourseId":row["CourseId"]
					,"Name":row["CourseName"]
					,"TotalUser":row["LearnPeopleCount"]
					,"TotalStaySec":row["StaySec"]
					,"TotalScore":row["TotalScore"]
					,"LearnCount":row["LearnCount"]
					,"Ranking":1
					,"Total":1
					,"PassCount":row["PassCount"]
					,"ExamCount":row["ExamCount"]
				})
			
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
@restapi.resource('/fulearn/V4/Group/<string:GroupName>/KeepUser/<DateStartHourToEndHour:DateStartHourToEndHour>')
class FulearnV4GroupKeepUser(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self, GroupName, headers=None):
		
		result = {"NewUser":100,"Day2KeepUser":90,"Day3KeepUser":80,"Day4KeepUser":70,"Day5KeepUser":60,"Day6KeepUser":50,"Day7KeepUser":40,"Day30KeepUser":10,"Day7Target":40,"Day30Target":10}
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
