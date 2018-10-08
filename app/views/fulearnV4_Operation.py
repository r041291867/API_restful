#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint,current_app, make_response,request, jsonify
from flask_restful import Resource, Api

from . import views_blueprint
from app.extensions import mysql2,restapi,cache
from app.utils import cache_key
import textwrap
import gzip
import logging
import json
import math
import random
from datetime import date,datetime as dt, timedelta

@restapi.resource('/fulearn/V4/Registration')
class FulearnV4Registration(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):

		result = []
		
		# default values
		year = 2017
		month = 3
		
		if year in request.args:
			year = request.args['year']
		if month in request.args:
			month = request.args['month']

		sql='''SELECT SUM(Count) FROM fulearn_4_view.Opration_by_RegisterPerson'''

		# connect
		conn = mysql2.connect()
		cursor = conn.cursor()

		try:
			cursor.execute(sql)
			rows = cursor.fetchall()
			
			for row in rows:
				result.append({
					'TotalNumber': row['SUM(Count)']
				})

		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/Registration failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()

		response = jsonify(result)
		response.status_code = 200

		return response


@restapi.resource('/fulearn/V4/Operation/ActivePeople/Online')
class FulearnV4ActivePeople(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):
		
		result = []

		sql = '''call fulearn_4_view.SP_GetMonthActivePepoleCount'''
		
		## connection to mysql
		conn = mysql2.connect()
		cursor = conn.cursor()

		try:
			cursor.execute(sql)
			rows = cursor.fetchall()

			if cursor.rowcount > 0:
				for row in rows:
					result.append({
						'Month': row['Month'],
						'Count': row['MonthActiveCount'],
						'total': row['TotalActiveCount']
					})

		
		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/ActivePeople failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()

		response = jsonify(result)
		response.status_code = 200
				
		return response

@restapi.resource('/fulearn/V4/Operation/CourseNum/Online')
class fulearnV4CourseNumOnline(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):

		today = dt.today()
		month = today.month
		year = today.year
		result = []

		sql = '''SELECT COUNT(*) AS month_total 
		FROM fulearn_4_view.Opration_by_Course 
		WHERE Year={0} AND Month={1}'''.format(year, month)

		## connection to mysql
		conn = mysql2.connect()
		cursor = conn.cursor()
		
		try :
			cursor.execute(sql)
			rows = cursor.fetchall()
			
			for row in rows:
				result.append({
					'month': month, # 月份
					'count': row['month_total'] or 0, #本月新增
					'total': 0
				})

		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/CourseNum failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()
		
		## connection to mysql
		conn = mysql2.connect()
		cursor = conn.cursor()
		sql2 = 'SELECT COUNT(*) AS courseTotal FROM fulearn_4_view.Opration_by_Course'

		try :
			cursor.execute(sql2)
			rows = cursor.fetchall()

			for row in rows:
				result[0]['total'] += row['courseTotal']								

		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/CourseNum failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()
			
		response = jsonify(result)
		response.status_code = 200
				
		return response

@restapi.resource('/fulearn/V4/Operation/CourseRank')
class FulearnV4CourseRank(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):

		result = []
		resp = make_response()
		length = 10

		if 'length' in request.args:
			length = request.args['length']

		sql = 'SELECT * FROM fulearn_4_view.auditTest GROUP BY CourseId ORDER BY SR DESC LIMIT {0}'
		sql = sql.format(length)

		## connection to mysql
		conn = mysql2.connect()
		cursor = conn.cursor()

		try :
			cursor.execute(sql)
			rows = cursor.fetchall()

			result = list(map(lambda row: {
				'CourseName': row['CourseName'],
				'Rank': row['SR']	
			}, rows))

		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/CourseRank failed')
			logging.getLogger('error_Logger').error(inst)
			resp = make_response()

		finally:
			cursor.close()
			conn.close()
			
		result = json.dumps(result , ensure_ascii=False)
		resp = make_response(result)
		return resp

@restapi.resource('/fulearn/V4/Operation/StudentRank')
class FulearnV4StudentRank(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):

		sql = '''SELECT * FROM fulearn_4_view.StudentRank 
		WHERE Name IS NOT NULL AND Source_UserName IS NOT NULL 
		ORDER BY TR DESC LIMIT 10'''

		conn = mysql2.connect()
		cursor = conn.cursor()
		result=[]
		try :
			cursor.execute(sql)
			rows = cursor.fetchall()

			for row in rows:
				result.append({
					"LearnGoal": row['TR'],
					"Name": row['Name'],
					"EmpNo": row['Source_UserName']
				})

		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/Persona/StudentRank failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()

		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
				
		return resp

@restapi.resource('/fulearn/V4/Operation/Persona/Gender')
class FulearnV4PersonaGender(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):
		
		sql = 'SELECT * FROM fulearn_4_view.Opration_by_imageSex'

		## connection to mysql
		conn = mysql2.connect()
		cursor = conn.cursor()
		result = []

		try :
			cursor.execute(sql)
			rows = cursor.fetchall()

			for row in rows:
				if row['Sex']=='0':
					tmp_dict = {
						'Gender': '女',
						'Count': int(row['Count'])
					}
				elif row['Sex']=='1':
					tmp_dict = {
						'Gender': '男',
						'Count': int(row['Count'])
					}
				else:
					tmp_dict = {
						'Gender': '外部',
						'Count': int(row['Count'])
					}
				
				result.append(tmp_dict)

		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/Persona/Gender failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()

		result = sorted(result, key=lambda k: k['Count'], reverse=True)
		result = json.dumps(result , ensure_ascii=False)
		resp = make_response(result)
				
		return resp

@restapi.resource('/fulearn/V4/Operation/Persona/Factory')
class FulearnV4PersonaFactory(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):

		length = 10
		if length in request.args:
			sql = '''SELECT * FROM fulearn_4_view.Opration_by_imageFactory 
			WHERE Factory IS NOT NULL ORDER BY COUNT DESC LIMIT %s''' % (length)
		else:					
			sql = '''SELECT * FROM fulearn_4_view.Opration_by_imageFactory 
			WHERE Factory IS NOT NULL ORDER BY COUNT DESC LIMIT %s''' % (length)
		
		result = []

		## connection to mysql
		conn = mysql2.connect()
		cursor = conn.cursor()

		try :
			cursor.execute(sql)
			rows = cursor.fetchall()

			for row in rows:
				if row['Factory'] is None:
					result.append({
						'Factory': '外部',
						'Count': int(row['Count'])
					})
				else:
					result.append({
						'Factory': row['Factory'],
						'Count': int(row['Count'])
					})

		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/Persona/Factory failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()
		
		result = sorted(result, key=lambda k: k['Count'], reverse=True) 
		result = json.dumps(result , ensure_ascii=False)
		resp = make_response(result)

		return resp

@restapi.resource('/fulearn/V4/Operation/Persona/Edu')
class FulearnV4PersonaEdu(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):
		sql = 'SELECT * FROM fulearn_4_view.Opration_by_imageEdu'

		result = []
		resp = make_response()

		## connection to mysql
		conn = mysql2.connect()
		cursor = conn.cursor()

		result.append({
			'Edu': '中職/高中',
			'Count': 0
		})
		try :
			cursor.execute(sql)
			rows = cursor.fetchall()

			for row in rows:
				count = int(row['Count'])
				if row['Edu'] is None:
					result.append({
						'Edu': '外部',
						'Count': count
					}) 
				elif row['Edu']=='中專/中職/技校':
					result[0]['Count'] += count
				elif row['Edu']== '高中/職高':
					result[0]['Count'] += count
				else:
					result.append({
						'Edu': row['Edu'],
						'Count': count
					}) 
		
		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/Persona/Edu failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()
		
		# sort result
		result = sorted(result, key=lambda k: k['Count'], reverse=True) 
		result = json.dumps(result , ensure_ascii=False)
		resp = make_response(result)

		return resp

@restapi.resource('/fulearn/V4/Operation/Persona/Age')
class FulearnV4PersonaAge(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):
		
		sql = 'SELECT * FROM fulearn_4_view.Opration_by_imageAge'

		## connection to mysql
		conn = mysql2.connect()
		cursor = conn.cursor()		
		
		## count people by age 
		ageDict = {
			"<25": 0,
			"25~30": 0,
			"30~35": 0,
			"35~40": 0,
			">40": 0
		}
		
		result = []

		try:
			cursor.execute(sql)
			rows = cursor.fetchall()

			for row in rows:
				age = row['Age']
				count = int(row['Count'])

				if age==None:
					pass
				elif age>40:
					ageDict[">40"] += count
				elif age>35 and age<=40:
					ageDict["35~40"] += count
				elif age>30 and age<=35:
					ageDict["30~35"] += count
				elif age>25 and age<=30:
					ageDict["25~30"] += count
				elif age<=25:
					ageDict["<25"] += count
				else:
					pass
				
		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/Persona/Age failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()
		
		for the_key, the_value in ageDict.items():
			result.append({
				"ageRange": the_key,
				"count": the_value
			})
		
		# sort value
		result = sorted(result, key=lambda k: k['count'], reverse=True) 
		
		if len(result)==0:
			result.append({
				'status': 'no data'
			})
		
		return result

@restapi.resource('/fulearn/V4/Operation/Persona/Management')
class FulearnV4PersonaManage(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):

		sql = 'SELECT * FROM fulearn_4_view.Opration_by_imageIsManage'

		result = []

		## connection to mysql
		conn = mysql2.connect()
		cursor = conn.cursor()

		try :
			cursor.execute(sql)
			rows = cursor.fetchall()

			for row in rows:
				if row['IsManage']=='Y':
					tmp_dict = {
						'Management': '有管理職',
						'Count': row['Count']
					}
				elif row['IsManage']=='N':
					tmp_dict = {
						'Management': '無管理職',
						'Count': row['Count']
					}
				else:
					tmp_dict = {
						'Management': '外部',
						'Count': int(row['Count'])
					}
				result.append(tmp_dict)

		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/Persona/Edu failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()
		
		result = sorted(result, key=lambda k: k['Count'], reverse=True)
		result = json.dumps(result , ensure_ascii=False)
		resp = make_response(result)

		return resp

@restapi.resource('/fulearn/V4/Operation/Keywords')
class FulearnV4Keyword(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):
		
		# default value for length
		length = 20
		sql = 'SELECT * FROM fulearn_4_view.Opration_by_KeyWordsCount ORDER BY QuerySum DESC LIMIT %s' % (length)
		
		result = []

		## connection to mysql
		conn = mysql2.connect()
		cursor = conn.cursor()

		try :
			cursor.execute(sql)
			rows = cursor.fetchall()
			for row in rows:
				result.append({
					"keyword": row["KeyWord"],
					"queryTimes": row["QuerySum"]
				})

		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/Persona/keyword failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()
		
		return result

@restapi.resource('/fulearn/V4/Operation/Map')
class FulearnV4MapMonth(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):
		# default value is current time
		today = dt.today()
		month = today.month
		day = today.day
		year= today.year

		result = []

		# default sql: last month people
		sql = '''SELECT * FROM fulearn_4_view.Opration_by_imageUserActiveChart 
		WHERE Year={0} AND Mon={1} AND LoginLon!=0 AND LoginLat!=0'''.format(year,month-1)

		sql2=''

		if 'year' in request.args:
			year = request.args['year']
			sql = '''SELECT * FROM fulearn_4_view.Opration_by_imageUserActiveChart 
			WHERE Year={0} AND Mon={1} AND LoginLon!=0 AND LoginLat!=0'''.format(year,month)

		if 'day' in request.args:
			sql = '''SELECT * FROM fulearn_4_view.Opration_by_imageUserActiveChart 
			WHERE Year={0} AND Mon={1} AND Day={2} AND LoginLon!=0 AND LoginLat!=0'''.format(year,month,day-1)
		
		if 'week' in request.args:
			# compute range of 7 days ago
			day_start = day - 7

			if(day_start<0):
				month = month - 1

				if(month in [1,3,5,7,8,10,12]):
					last_month_day_start = 31+day_start
					last_month_day_end = 31
				else:
					last_month_day_start = 30+day_start
					last_month_day_end = 30

				# 上月
				sql = '''SELECT * FROM fulearn_4_view.Opration_by_imageUserActiveChart 
				WHERE Year={0} AND Mon={1} 
				AND Day>={2} AND Day<={3}
				AND LoginLon!=0 AND LoginLat!=0'''.format(year,month,last_month_day_start, last_month_day_end)

				# 本月
				sql2 = '''SELECT * FROM fulearn_4_view.Opration_by_imageUserActiveChart 
				WHERE Year={0} AND Mon={1} 
				AND Day>={2} AND Day<={3} 
				AND LoginLon!=0 AND LoginLat!=0'''.format(year, month+1, 1, day)

			else:
				sql = '''SELECT * FROM fulearn_4_view.Opration_by_imageUserActiveChart 
				WHERE Year={0} AND Mon={1} AND Day>={2} 
				AND Day<={3} AND LoginLon!=0 AND LoginLat!=0'''.format(year,month,day_start, day)

		## connection to mysql
		conn = mysql2.connect()
		cursor = conn.cursor()

		try :
			if(sql2==''):
				cursor.execute(sql)
				rows = cursor.fetchall()

				if cursor.rowcount > 0:
					for row in rows:
						result.append({
							'lan': row['LoginLat'],
							'lon': row['LoginLon'],
							'count': row['OnlineSum'],
						})
			else:
				cursor.execute(sql)
				rows = cursor.fetchall()

				if cursor.rowcount > 0:
					for row in rows:
						result.append({
							'lan': row['LoginLat'],
							'lon': row['LoginLon'],
							'count': row['OnlineSum'],
						})
				
				cursor.execute(sql2)
				rows = cursor.fetchall()
				
				if cursor.rowcount > 0:
					for row in rows:
						result.append({
							'lan': row['LoginLat'],
							'lon': row['LoginLon'],
							'count': row['OnlineSum'],
						})
					
		except Exception as inst:
			logging.getLogger('error_Logger').error('/fulearn/V4/Operation/Map failed')
			logging.getLogger('error_Logger').error(inst)

		finally:
			cursor.close()
			conn.close()
		
		return result