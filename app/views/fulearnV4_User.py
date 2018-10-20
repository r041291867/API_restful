#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint,current_app, make_response, jsonify
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


@restapi.resource('/fulearn/V4/User/<string:UserId>')
class FulearnV4UserInfo(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, UserId, headers=None):

		result = {}

		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetUserInfoByUserId(%s);",UserId)
			rows = cursor.fetchall()
			result["UserId"] = rows[0]["UserId"]
			result["Name"] = rows[0]["Name"]
			result["Old"] = rows[0]["Age"]
			result["Jobyears"] = math.ceil(rows[0]["JobTime"]/52)
			result["Edu"] = rows[0]["Edu"]
			result["Factory"] = rows[0]["Area"]
			result["Photo"] = "http://iedu.foxconn.com:8080/head_pic/{0}.jpg".format(rows[0]["UserId"])

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
@restapi.resource('/fulearn/V4/User/<string:UserId>/RecommendCourses')
class FulearnV4UserRecommendCourses(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, UserId, headers=None):

		result = {"RecommnedCourses":[]}

		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetUserRecommendCoursesByUserId(%s);",UserId)
			rows = cursor.fetchall()
			for row in rows :
				result['RecommnedCourses'].append(
					{"CourseId":row['CourseId']
					,"Name":row['CourseName']})

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
@restapi.resource('/fulearn/V4/User/<string:UserId>/LearnCourses')
class FulearnV4UserLearnCourses(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, UserId, headers=None):

		result = {"LearnCourses":[]}

		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetUserLearnCoursesByUserId(%s);",UserId)
			rows = cursor.fetchall()

			for row in rows :
				result['LearnCourses'].append(
					{"CourseId":row['CourseId']
					,"Name":row['CourseName']
					,"TotalStaySec":row['TotalStaySec']
					,"Score":row["score"]
					,"credit": 1 if row["pass"] == 'Pass' else 0
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
@restapi.resource('/fulearn/V4/User/<string:UserId>/LearnAlert/<StartDateToEndDate:Dates>')
class FulearnV4UserAlertDates(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, UserId,Dates, headers=None):

		result = {"LearnCourses":[]}
		StartDate = Dates['Start']

		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute("call fulearn_4_view.SP_GetUserAlertByUserIdAndDate(%s,%s,%s);"
				,[UserId
				,Dates['Start'].strftime('%Y-%m-%d')
				,Dates['End'].strftime('%Y-%m-%d')])
			rows = cursor.fetchall()

			for row in rows :
				if row['Date'] != StartDate :
					while StartDate < dt.strptime(row['Date'],'%Y-%m-%d') :
						result['LearnCourses'].append({"Date":StartDate.strftime('%Y-%m-%d')
						,"TotalLearnSec":0
						,"AlertLevel":0
						})
						StartDate = StartDate + timedelta(days=1)
				result['LearnCourses'].append({"Date":row['Date']
					,"TotalLearnSec":row['TotalStaySec']
					,"AlertLevel":row['AlertLevel']})
				StartDate = StartDate + timedelta(days=1)
			while StartDate <= Dates['End'] :
				result['LearnCourses'].append({"Date":StartDate.strftime('%Y-%m-%d')
						,"TotalLearnSec":0
						,"AlertLevel":0
						})
				StartDate = StartDate + timedelta(days=1)


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

@restapi.resource('/fulearn/V4/User/<string:UserId>/LearnAbility')
class FulearnV4UserLearnAbility(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, UserId, headers=None):
		# {
		# 	id: F444,
		# 	TotalLearnSec: 2455
		# 	Credit: 123
		# 	AbilityScore: 45
		# 	AbilityRank:
		# 	AbilityGrade: 1
		# }
		result = []
		sql = '''SELECT * FROM fulearn_4_view.LearnAbility WHERE emp_no="{0}"'''.format(UserId)

		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute(sql)
			rows = cursor.fetchall()
			for row in rows :
				abilities = {
					'id': row['emp_no'],
					'TotalLearnSec': row['TotalLearnSec'],
					'Credit': row['Credit'],
					'AbilityScore': row['AbilityScore'],
					'AbilityRank': row['AbilityRank'],
					'AbilityGrade': row['AbilityGrade']
				}
				result.append(abilities)

			cursor.close()
			conn.close()
		except Exception as inst:
			logging.getLogger('error_Logger').error('FulearnV4 Query LearnAbility Err')
			logging.getLogger('error_Logger').error(inst)

		resp = make_response(
			json.dumps(result , ensure_ascii=False)
		)

		resp.headers.extend(headers or {})

		return resp

@restapi.resource('/fulearn/V4/User/<string:UserId>/LearnProgress')
class FulearnV4UserLearnProgress(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, UserId, headers=None):

		result = []
		sql = '''SELECT * FROM fulearn_4_view.LearnProgress WHERE emp_no="{0}"'''.format(UserId)

		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute(sql)
			rows = cursor.fetchall()

			for row in rows :
				progress = {
					'id': UserId,
					'CourseName': row['CourseName'],
					'CourseNo': row['CourseNo'],
					'Creator': row['Creator'],
					'CompletionTime': row['CompletionTime'],
					'CompletionStatus': row['CompletionStatus']
				}
				result.append(progress)

			cursor.close()
			conn.close()
		except Exception as inst:
			logging.getLogger('error_Logger').error('FulearnV4 Query LearnProgress Err')
			logging.getLogger('error_Logger').error(inst)

		resp = make_response(
			json.dumps(result , ensure_ascii=False)
		)

		resp.headers.extend(headers or {})

		return resp

@restapi.resource('/fulearn/V4/User/<string:UserId>/LearnCategoryStat')
class FulearnV4UserLearnCategoryStat(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, UserId, headers=None):

		result = []
		sql = 'SELECT * FROM fulearn_4_view.LearnCategoryStat WHERE emp_no="{0}"'.format(UserId)
		conn = mysql2.connect()
		cursor = conn.cursor()

		try :
			cursor.execute(sql)
			rows = cursor.fetchall()
			for row in rows :
				print(row)
				result.append({
					'id': row['emp_no'],
					'Class1': row['Class1'],
					'CourseCount': row['CourseCount'],
					'TotalCredit': row['TotalCredit']
				})
		except Exception as inst:
			logging.getLogger('error_Logger').error('FulearnV4 Query LearnAbility Err')
			logging.getLogger('error_Logger').error(inst)

		finally :
			cursor.close()
			conn.close()

		resp = make_response(
			json.dumps(result , ensure_ascii=False)
		)

		resp.headers.extend(headers or {})

		return resp


@restapi.resource('/fulearn/V4/User/<string:UserId>/LearnCourseStat')
class FulearnV4UserLearnCourseStat(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, UserId, headers=None):

		result = []
		sql = 'SELECT * FROM fulearn_4_view.LearnCourseStat WHERE emp_no="{0}"'.format(UserId)

		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute(sql)
			rows = cursor.fetchall()
			for row in rows :
				courseStat = {
					'id': row['emp_no'],
					'CourseName': row['CourseName'],
					'CourseID': row['CourseID'],
					'CourseCredit': row['CourseCredit'],
					'Score': row['Score'],
					'ScoreAvg': row['ScoreAvg'],
					'LearnTimeSum': row['LearnTimeSum'],
					'LearnTimeAvg': row['LearnTimeAvg']
				}
				result.append(courseStat)

		except Exception as inst:
			logging.getLogger('error_Logger').error('FulearnV4 Query LearnAbility Err')
			logging.getLogger('error_Logger').error(inst)

		resp = make_response(
			json.dumps(result , ensure_ascii=False)
		)

		resp.headers.extend(headers or {})

		return resp

@restapi.resource('/fulearn/V4/User/<string:UserId>/Activity')
class FulearnV4UserActivity(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, UserId, headers=None):

		result = []

		sql = '''
			SELECT vp.percent, vp.activity, fl.Source_UserName, vp.UserId
			FROM fulearn_4_view.personal_by_activity as vp
			LEFT JOIN fulearn_4.log_data_person as fl
			ON vp.UserId = fl.UserID
		'''

		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute(sql)
			rows = cursor.fetchall()
			for row in rows :
				if row['Source_UserName'] == UserId:
					result.append({
						'Emp_no': row['Source_UserName'],
						'Activity': row['percent']
					})

		except Exception as inst:
			logging.getLogger('error_Logger').error('FulearnV4 Query LearnAbility Err')
			logging.getLogger('error_Logger').error(inst)

		resp = make_response(
			json.dumps(result , ensure_ascii=False)
		)

		resp.headers.extend(headers or {})

		return resp



@restapi.resource('/fulearn/V4/User/<string:UserId>/LearnCourseErr')
class FulearnV4UserLearnCourseErr(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, UserId, headers=None):

		result = []
		sql = '''SELECT * FROM fulearn_4_view.LearnCourseErr WHERE emp_no="{0}"'''.format(UserId)

		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute(sql)
			rows = cursor.fetchall()

			result = list(map(lambda row: {
				'id': row['emp_no'],
				'CourseName': row['CourseName'],
				'CourseID': row['CourseID'],
				'ErrorItem': row['ErrorItem'],
				'ErrorCount': row['ErrorCount']
			}, rows))

			cursor.close()
			conn.close()

		except Exception as inst:
			logging.getLogger('error_Logger').error('FulearnV4 Query LearnCourseErr Err')
			logging.getLogger('error_Logger').error(inst)

		resp = make_response(
			json.dumps(result , ensure_ascii=False)
		)

		resp.headers.extend(headers or {})

		return resp

# for new fulearn API

@restapi.resource('/fulearn/V4/person/info')
class FulearnV4PersonInfo(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default value of groups
        jobID = 'F0300120'

        # store result
        result = []

        # 如果網址列有參數就取代
        if 'id' in request.args:
            jobID = request.args['id']

        sql = '''SELECT * FROM fulearn_4_view.PersonInfo
        WHERE id = "{0}"
        '''.format(jobID)

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if cursor.rowcount>0:
                for row in rows:
                    result.append({
                        'id': row['id'],
                        'name': row['name'],
                        'jobLevel': row['jobLevel'],
                        'education': row['education'],
                        'factoryArea':row['factoryArea'],
                        'profession': row['profession'],
                        'Group1': row['Group1'],
                        'Group2': row['Group2'],
                        'Group3': row['Group3'],
                        'position': row['position'],
                        'gender': row['gender'],
                        'seniority': row['seniority'],
                        'age': row['age']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/department/learnedCourse failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result


@restapi.resource('/fulearn/V4/person/learnAbility')
class FulearnV4PersonLearnAbility(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default value of groups
        jobID = 'F0300120'

        # store result
        result = []

        # 如果網址列有參數就取代
        if 'id' in request.args:
            jobID = request.args['id']

        sql = '''SELECT * FROM fulearn_4_view.PersonLearnAbility
        WHERE id = "{0}"
        '''.format(jobID)

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if cursor.rowcount>0:
                for row in rows:
                    result.append({
                        'id': row['id'],
                        'gainedCredits': str(row['gainedCredits']),
                        'pointer': row['pointer'],
                        'wonMember': round(float(row['wonMember']), 2),
                        'abilityLevel':row['abilityLevel'],
                        'ability': round(float(row['ability']), 2)
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/department/learnedCourse failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/fulearn/V4/person/abliltyRadar')
class FulearnV4PersonAbliltyRadar(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default value of groups
        jobID = 'F0300120'

        # store result
        result = []

        # 如果網址列有參數就取代
        if 'id' in request.args:
            jobID = request.args['id']

        sql = '''SELECT * FROM fulearn_4_view.PersonAbliltyRadar
        WHERE id = "{0}"
        '''.format(jobID)

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if cursor.rowcount>0:
                for row in rows:
                    result.append({
                        'id': row['id'],
                        'courceTypeID': str(row['courceTypeID']),
                        'courceTypeName': row['courceTypeName'],
                        'percent': row['percent'],
                        'courseCredits': str(row['courseCredits'])
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/department/learnedCourse failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/fulearn/V4/person/learnedTaskInfo')
class FulearnV4PersonLearnedTaskInfo(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default value of groups
        jobID = 'F0300120'

        # store result
        result = []

        # 如果網址列有參數就取代
        if 'id' in request.args:
            jobID = request.args['id']

        sql = '''SELECT * FROM fulearn_4_view.PersonLearnedTaskInfo
        WHERE id = "{0}"
        '''.format(jobID)

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if cursor.rowcount>0:
                for row in rows:
                    result.append({
                        'id': row['id'],
                        'CourseId': row['CourseId'],
						'finishedCourseName': row['finishedCourseName'],
                        'personLearnTime': row['personLearnTime'],
                        'avgCourseLearnTime': row['avgCourseLearnTime'],
                        'learnPeople': row['learnPeople'],
                        'rank': row['rank'],
                        'personCourseScore': row['personCourseScore'],
                        'passingRate': str(round((row['passingRate']*100), 2))+"%",
                        'avgCourseScore': row['avgCourseScore']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/department/learnedCourse failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result


@restapi.resource('/fulearn/V4/person/weeklyLearnTime')
class FulearnV4PersonWeeklyLearnTime(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        # default values
        year = '2017'
        jobID = 'F0300120'

        # store result
        result = []

        # update parameters
        if 'year' in request.args:
            year = request.args['year']

        if 'id' in request.args:
            jobID = request.args['id']

        sql=''' SELECT * FROM fulearn_4_view.PersonWeeklyLearnTime
        WHERE id = "{0}"
        AND year="{1}"
        '''.format(jobID, year)

        print(sql)

        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            # sql
            cursor.execute(sql)
            rows = cursor.fetchall()

            ## if it is not empty set
            if cursor.rowcount>0:
                for r in rows:
                    result.append({
                        'id': r['id'],
                        'year': r['year'],
                        'period': r['period'],
                        'personLearnTime': r['personLearnTime'],
                        'wholeLearnTime': r['wholeLearnTime']
                    })
            else:
                pass
        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/person/weeklyLearnTime failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200

        return result



@restapi.resource('/fulearn/V4/person/recomCourseRoute')
class FulearnV4PersonRecomCourseRoute(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        # default values
        jobID = 'F0300120'

        # store result
        result = []

        # update parameters
        if 'id' in request.args:
            jobID = request.args['id']

        sql=''' SELECT * FROM fulearn_4_view.PersonRecomCourseRoute
        WHERE id = "{0}"
        '''.format(jobID)

        print(sql)

        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            # sql
            cursor.execute(sql)
            rows = cursor.fetchall()

            ## if it is not empty set
            if cursor.rowcount>0:
                for r in rows:
                    result.append({
                        'id': r['id'],
                        'courseOrder': r['courseOrder'],
                        'courseName': r['courseName'],
                        'subcourseName': r['subcourseName']
                    })
            else:
                pass
        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/person/weeklyLearnTime failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200

        return result
