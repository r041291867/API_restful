#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint, make_response, json
from flask_restful import Resource, Api,reqparse

from . import views_blueprint
from app.extensions import restapi,cache,mysql
from app.utils import cache_key
from datetime import date, datetime
import logging
import textwrap
from math import ceil
from decimal import *
from MySQLdb import cursors
import json

#post_parser = reqparse.RequestParser()
#post_parser.add_argument(
#    'StartDate-EndDate', dest='username',
#    location='args', required=True,
#    help='The user\'s username',
#)


@restapi.resource('/iStudy/V1')
class iStudy(Resource):
	#@cache.cached(timeout=600, key_prefix='/iStudy', unless=None)
	def get(self):
		print("Hello No Cache")
		logging.info('Hello is Logging')
		return {'hello': 'world'}

@restapi.resource('/iStudy/V1/LearnCountByYearMonth')
class LearnYearMonthCount(Resource):
	@cache.cached(timeout=600, key_prefix='/iStudy/V1/LearnCountByYearMonth', unless=None)
	def get(self):
#		conn = mysql.connect()
#		cursor =conn.cursor()
#		cursor.execute("select Year,YearLearnCount,Month,MonthLearnCount from IEDB.LearnCount_YearMonth_View order by Month;")
#		data = cursor.fetchone()
		
		
		return {'hello': 'world'}
		
@restapi.resource('/iStudy/V1/LearnCountByWeekDay/<StartMonthToEndMonth:starttoend>')
class LearnCountByWeekDay(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self,starttoend):
		StartMonth = starttoend['Start'] if not starttoend is None else ''
		EndMonth = starttoend['End'] if not starttoend is None else ''
		cursor =mysql.connection.cursor(cursors.DictCursor)
		cursor.execute("call IEDB.GetWeekDayLearnCount(%s,%s)",(StartMonth,EndMonth))
		rows = cursor.fetchall()
		cursor.close()
		result = []
		for onerow in rows :
			SMonthObj = None
			EMonthObj = None
			for oneMonth in result :
				if oneMonth['Month'] == onerow['StartMonth'] :
					SMonthObj = oneMonth
				if oneMonth['Month'] == onerow['EndMonth'] :
					EMonthObj = oneMonth
			if SMonthObj is None :
				SMonthObj = {'Month':onerow['StartMonth'],"Weeks":[] }
				result.append(SMonthObj)
			if EMonthObj is None :
				EMonthObj = {'Month':onerow['EndMonth'],"Weeks":[] }
				result.append(SMonthObj)
			Week1 = {"WeekLearnCount":int(onerow['WeekLearnCount']),"Days":[]}
			Week2 = {"WeekLearnCount":int(onerow['WeekLearnCount']),"Days":[]}
			Day1 = {"Date":datetime.strftime(onerow['WeekDay1'],'%Y-%m-%d'),"LearnCount":int(onerow['Day1LearnCount']) if not onerow.get('Day1LearnCount') is None else 0}
			Day2 = {"Date":datetime.strftime(onerow['WeekDay2'],'%Y-%m-%d'),"LearnCount":int(onerow['Day2LearnCount']) if not onerow.get('Day2LearnCount') is None else 0}
			Day3 = {"Date":datetime.strftime(onerow['WeekDay3'],'%Y-%m-%d'),"LearnCount":int(onerow['Day3LearnCount']) if not onerow.get('Day3LearnCount') is None else 0}
			Day4 = {"Date":datetime.strftime(onerow['WeekDay4'],'%Y-%m-%d'),"LearnCount":int(onerow['Day4LearnCount']) if not onerow.get('Day4LearnCount') is None else 0}
			Day5 = {"Date":datetime.strftime(onerow['WeekDay5'],'%Y-%m-%d'),"LearnCount":int(onerow['Day5LearnCount']) if not onerow.get('Day5LearnCount') is None else 0}
			Day6 = {"Date":datetime.strftime(onerow['WeekDay6'],'%Y-%m-%d'),"LearnCount":int(onerow['Day6LearnCount']) if not onerow.get('Day6LearnCount') is None else 0}
			Day7 = {"Date":datetime.strftime(onerow['WeekDay7'],'%Y-%m-%d'),"LearnCount":int(onerow['Day7LearnCount']) if not onerow.get('Day7LearnCount') is None else 0}
			
			if datetime.strftime(onerow['WeekDay1'],'%Y-%m-%d').startswith(onerow['StartMonth']) :
				Week1['Days'].append(Day1)
			else :
				Week2['Days'].append(Day1)
			if datetime.strftime(onerow['WeekDay2'],'%Y-%m-%d').startswith(onerow['StartMonth']) :
				Week1['Days'].append(Day2)
			else :
				Week2['Days'].append(Day2)
			if datetime.strftime(onerow['WeekDay3'],'%Y-%m-%d').startswith(onerow['StartMonth']) :
				Week1['Days'].append(Day3)
			else :
				Week2['Days'].append(Day3)
			if datetime.strftime(onerow['WeekDay4'],'%Y-%m-%d').startswith(onerow['StartMonth']) :
				Week1['Days'].append(Day4)
			else :
				Week2['Days'].append(Day4)
			if datetime.strftime(onerow['WeekDay5'],'%Y-%m-%d').startswith(onerow['StartMonth']) :
				Week1['Days'].append(Day5)
			else :
				Week2['Days'].append(Day5)
			if datetime.strftime(onerow['WeekDay6'],'%Y-%m-%d').startswith(onerow['StartMonth']) :
				Week1['Days'].append(Day6)
			else :
				Week2['Days'].append(Day6)
			if datetime.strftime(onerow['WeekDay7'],'%Y-%m-%d').startswith(onerow['StartMonth']) :
				Week1['Days'].append(Day7)
			else :
				Week2['Days'].append(Day7)
		if len(Week1['Days']) > 0 :
			SMonthObj['Weeks'].append(Week1)
		if len(Week2['Days']) > 0 :
			EMonthObj['Weeks'].append(Week2)
		resp = make_response(json.dumps(result ,ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
#		return {'StartMonth':StartMonth,'EndMonth':EndMonth}
#		return {'hello': 'world'}
		
@restapi.resource('/iStudy/V1/LearnCountByHour/<StartDateToEndDate:starttoend>')
class LearnCountByHour(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self,starttoend):
		StartDate = starttoend['Start'] if not starttoend is None else ''
		EndDate = starttoend['End'] if not starttoend is None else ''
#		return {'hello': 'world'}
		cursor =mysql.connection.cursor(cursors.DictCursor)
		cursor.execute("call IEDB.GetHourLearnCount(%s,%s)",(StartDate,EndDate))
		rows = cursor.fetchall()
		cursor.close()
		result = []
		lastDay = ''
		lastDayObj = ''
		for onerow in rows :
			if onerow["Day"] != lastDay :
				lastDayObj = {"Day":onerow["Day"],"Hours":[]}
				result.append(lastDayObj)
			HourObj = {"Hour":onerow["Hour"],"LearnCount":int(onerow["LearnCount"])}
			lastDayObj["Hours"].append(HourObj)
		resp = make_response(json.dumps(result ,ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
		
@restapi.resource('/iStudy/V1/Learn_Video_Code')
class Learn_Video_Code(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):
#		return {'hello': 'world'}
		cursor =mysql.connection.cursor(cursors.DictCursor)
		cursor.execute(textwrap.dedent('''
select ParentType,ParentName,ParentTypeDiff,Subtype,SubtypeName,SubtypeDiff,VideoCode,VideoName,VideoCodeDiff
from IEDB.LearnSec_VideoInfo_View
order by ParentType,Subtype,VideoCode'''))
		
		rows = cursor.fetchall()
		cursor.close()
		result = []
		lastParentType = ''
		lastSubtype = ''
		lastParentObj = None
		lastSubtypeObj = None
		for onerow in rows :
			if onerow['ParentType'] != lastParentType :
				lastParentObj = {"ParentId":onerow['ParentType'],"ParentName":onerow["ParentName"]
					,"Subtypes":[],"DiffTime":int(onerow["ParentTypeDiff"])}
				result.append(lastParentObj)
			if onerow['Subtype'] != lastSubtype :
				lastSubtypeObj = {"SubtypeId":onerow["Subtype"],"SubtypeName":onerow["SubtypeName"]
					,"Videos":[],"DiffTime":int(onerow["SubtypeDiff"])}
				lastParentObj["Subtypes"].append(lastSubtypeObj)
			Video = {"VideoCode":onerow["VideoCode"],"VideoName":onerow["VideoName"]
				,"DiffTime":int(onerow["VideoCodeDiff"])}
			lastSubtypeObj["Videos"].append(Video)
			lastParentType = onerow["ParentType"]
			lastSubtype = onerow["Subtype"]
		resp = make_response(json.dumps(result ,ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp

@restapi.resource('/iStudy/V1/ActiveUser')
class ActiveUser(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):
#		return {'hello': 'world'}
		cursor =mysql.connection.cursor(cursors.DictCursor)
		cursor.execute(textwrap.dedent('''
select cast(login as char(5)) as login,n as n,cast(cast(date as date) as char(10)) as datestr
from iStudy_View.ActiveUser
order by date desc,login'''))

		rows = cursor.fetchall()
		cursor.close()
		result = []
		month = []
		year = []
		lastDateObj = None
		lastYearObj = None
		lastMonthObj = None
		for onerow in rows :
			yearstr = onerow['datestr'][0:4]
			monthstr = onerow['datestr'][5:2]
			if lastYearObj is None or lastYearObj['Year'] != yearstr :
				lastYearObj = {'Year':yearstr,'Months':[]}
				lastMonthObj = {'Month':monthstr,'Dates':[]}
				lastDateObj = {'Date':onerow['datestr'],'UnactiveUserCount':0,'ActiveUserCount':0,'DormancyUserCount':0}
				lastMonthObj['Dates'].append(lastDateObj)
				lastYearObj['Months'].append(lastMonthObj)
				result.append(lastYearObj)
			if lastMonthObj['Month'] != monthstr :
				lastMonthObj = {'Month':monthstr,'Dates':[]}
				lastDateObj = {'Date':onerow['datestr'],'UnactiveUserCount':0,'ActiveUserCount':0,'DormancyUserCount':0}
				lastMonthObj['Dates'].append(lastDateObj)
				lastYearObj['Months'].append(lastMonthObj)
			if lastDateObj['Date'] != onerow['datestr'] :
				lastDateObj = {'Date':onerow['datestr'],'UnactiveUserCount':0,'ActiveUserCount':0,'DormancyUserCount':0}
				lastMonthObj['Dates'].append(lastDateObj)
			if onerow['login'] == '1' :
				lastDateObj['ActiveUserCount'] = int(onerow['n'])
			elif onerow['login'] == '2' :
				lastDateObj['UnactiveUserCount'] = int(onerow['n'])
			elif onerow['login'] == '12' :
				lastDateObj['DormancyUserCount'] = int(onerow['n'])
				
		resp = make_response(json.dumps(result ,ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
		
@restapi.resource('/iStudy/V1/DeviceUser')
class DeviceUser(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):
#		return {'hello': 'world'}
		cursor =mysql.connection.cursor(cursors.DictCursor)
		cursor.execute(textwrap.dedent('''
SELECT cast(DeviceKind as char(5)) as DeviceKind,n,cast(cast(date as date) as char(10)) as datestr
from iStudy_View.Device
order by date desc,DeviceKind;'''))
		rows = cursor.fetchall()
		cursor.close()
		result = []
		month = []
		year = []
		lastDateObj = None
		lastYearObj = None
		lastMonthObj = None
		for onerow in rows :
			yearstr = onerow['datestr'][0:4]
			monthstr = onerow['datestr'][5:2]
			if lastYearObj is None or lastYearObj['Year'] != yearstr :
				lastYearObj = {'Year':yearstr,'Months':[]}
				lastMonthObj = {'Month':monthstr,'Dates':[]}
				lastDateObj = {'Date':onerow['datestr'],'IosUserCount':0,'AndroidUserCount':0,'BothuseUserCount':0}
				lastMonthObj['Dates'].append(lastDateObj)
				lastYearObj['Months'].append(lastMonthObj)
				result.append(lastYearObj)
			if lastMonthObj['Month'] != monthstr :
				lastMonthObj = {'Month':monthstr,'Dates':[]}
				lastDateObj = {'Date':onerow['datestr'],'IosUserCount':0,'AndroidUserCount':0,'BothuseUserCount':0}
				lastMonthObj['Dates'].append(lastDateObj)
				lastYearObj['Months'].append(lastMonthObj)
			if lastDateObj['Date'] != onerow['datestr'] :
				lastDateObj = {'Date':onerow['datestr'],'IosUserCount':0,'AndroidUserCount':0,'BothuseUserCount':0}
				lastMonthObj['Dates'].append(lastDateObj)
			if onerow['login'] == '1' :
				lastDateObj['AndroidUserCount'] = int(onerow['n'])
			elif onerow['login'] == '2' :
				lastDateObj['IosUserCount'] = int(onerow['n'])
			elif onerow['login'] == '12' :
				lastDateObj['BothuseUserCount'] = int(onerow['n'])
				
		resp = make_response(json.dumps(result ,ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
		
		
		
@restapi.resource('/iStudy/V1/CourseLearnInfoByBg')
class CourseLearnInfoByBg(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self):
#		return {'hello': 'world'}
		cursor =mysql.connection.cursor(cursors.DictCursor)
		cursor.execute('call iStudy_View.SP_GetCourseLearnInfo();')
		CourseRows = cursor.fetchall()
		cursor.nextset()
		ClassRows = cursor.fetchall()
		cursor.close()
		result = []
		
		LastClassObj1 = None
		LastClassObj2 = None
		LastClassObj3 = None
		LastCourseObj = None
		for OneCourseRow in CourseRows :
			findClass1Obj = False
			findClass2Obj = False
			findClass3Obj = False
			if LastClassObj1 is None or LastClassObj1['ClassId1'] != oneCourseRow['ClassId1'] :
				LastClassObj1 = {"ClassId":oneCourseRow["ClassId1"],"SubClasses":[],"BGs":[]}
				LastClassObj2 = {"ClassId":oneCourseRow["ClassId2"],"SubClasses":[],"BGs":[]}
				LastClassObj3 = {"ClassId":oneCourseRow["ClassId3"],"SubCourses":[],"BGs":[]}
				LastCourseObj = {"CourseId":oneCourseRow["CourseId"],"CourseName":oneCourseRow["CourseName"],"BGs":[]}
				findClass1Obj = True
				findClass2Obj = True
				findClass3Obj = True
				result.append(LastClassObj1)
				LastClassObj1['SubClasses'].append(LastClassObj2)
				LastClassObj2['SubClasses'].append(LastClassObj3)
				LastClassObj3['SubCourses'].append(LastCourseObj)
			if LastClassObj2['ClassId'] != oneCourseRow['ClassId2'] :
				LastClassObj2 = {"ClassId":oneCourseRow["ClassId2"],"SubClasses":[],"BGs":[]}
				LastClassObj3 = {"ClassId":oneCourseRow["ClassId3"],"SubCourses":[],"BGs":[]}
				LastCourseObj = {"CourseId":oneCourseRow["CourseId"],"CourseName":oneCourseRow["CourseName"],"BGs":[]}
				findClass2Obj = True
				findClass3Obj = True
				LastClassObj1['SubClasses'].append(LastClassObj2)
				LastClassObj2['SubClasses'].append(LastClassObj3)
				LastClassObj3['SubCourses'].append(LastCourseObj)
			if LastClassObj3['ClassId'] != oneCourseRow['ClassId3'] :
				LastClassObj3 = {"ClassId":oneCourseRow["ClassId3"],"SubCourses":[],"BGs":[]}
				LastCourseObj = {"CourseId":oneCourseRow["CourseId"],"CourseName":oneCourseRow["CourseName"],"BGs":[]}
				findClass3Obj = True
				LastClassObj2['SubClasses'].append(LastClassObj3)
				LastClassObj3['SubCourses'].append(LastCourseObj)
			if LastCourseObj['CourseId'] != oneCourseRow["CourseId"] :
				LastCourseObj = {"CourseId":oneCourseRow["CourseId"],"CourseName":oneCourseRow["CourseName"],"BGs":[]}
				LastClassObj3['SubCourses'].append(LastCourseObj)
			if findClass1Obj :
				find = False
				for oneClassRow in ClassRows :
					if oneClassRow['ClassId'] == LastClassObj1['ClassId'] :
						find = True
						if oneClassRow['BG'] == 'All' :
							LastClassObj1['ClassName'] = oneClassRow['ClassName']
							LastClassObj1['TotalStaySec'] = oneClassRow['TotalStaySec']
							LastClassObj1['TotalLearnPeople'] = oneClassRow['TotalLearnPeople']
							LastClassObj1['TotalLearn'] = oneClassRow['TotalLearn']
							LastClassObj1['AvgScore'] = 0 if oneClassRow.get('AvgScore')is None else oneClassRow['AvgScore']
							LastClassObj1['PassPeople'] = 0 if oneClassRow.get('Pass') is None else oneClassRow['Pass']
						else :
							LastClassObj1['BGs'].append({'BG':oneClassRow['BG']
								,'TotalStaySec':oneClassRow['TotalStaySec']
								,'TotalLearnPeople':oneClassRow['TotalLearnPeople']
								,'TotalLearn':oneClassRow['TotalLearn']
								,'AvgScore':0 if oneClassRow.get('AvgScore')is None else oneClassRow['AvgScore']
								,'PassPeople':0 if oneClassRow.get('PassPeople')is None else oneClassRow['PassPeople']
							})
					if find and oneClassRow['ClassId'] != LastClassObj1['ClassId'] :
						break
			if findClass2Obj :
				find = False
				for oneClassRow in ClassRows :
					if oneClassRow['ClassId'] == LastClassObj2['ClassId']:
						find = True
						if oneClassRow['BG'] == 'All' :
							LastClassObj2['ClassName'] = oneClassRow['ClassName']
							LastClassObj2['TotalStaySec'] = oneClassRow['TotalStaySec']
							LastClassObj2['TotalLearnPeople'] = oneClassRow['TotalLearnPeople']
							LastClassObj2['TotalLearn'] = oneClassRow['TotalLearn']
							LastClassObj2['AvgScore'] = 0 if oneClassRow.get('AvgScore')is None else oneClassRow['AvgScore']
							LastClassObj2['PassPeople'] = 0 if oneClassRow.get('PassPeople')is None else oneClassRow['PassPeople']
						else :
							LastClassObj2['BGs'].append({'BG':oneClassRow['BG']
								,'TotalStaySec':oneClassRow['TotalStaySec']
								,'TotalLearnPeople':oneClassRow['TotalLearnPeople']
								,'TotalLearn':oneClassRow['TotalLearn']
								,'AvgScore':0 if oneClassRow.get('AvgScore')is None else oneClassRow['AvgScore']
								,'PassPeople':0 if oneClassRow.get('PassPeople')is None else oneClassRow['PassPeople']
								
							})
					if find and oneClassRow['ClassId'] != LastClassObj2['ClassId'] :
						break
			if findClass3Obj :
				find = False
				for oneClassRow in ClassRows :
					if oneClassRow['ClassId'] == LastClassObj3['ClassId']:
						find = True
						if oneClassRow['BG'] == 'All' :
							LastClassObj3['ClassName'] = oneClassRow['ClassName']
							LastClassObj3['TotalStaySec'] = oneClassRow['TotalStaySec']
							LastClassObj3['TotalLearnPeople'] = oneClassRow['TotalLearnPeople']
							LastClassObj3['TotalLearn'] = oneClassRow['TotalLearn']
							LastClassObj3['AvgScore'] = 0 if oneClassRow.get('AvgScore')is None else oneClassRow['AvgScore']
							LastClassObj3['PassPeople'] = 0 if oneClassRow.get('Pass') is None else oneClassRow['Pass']
						else :
							LastClassObj3['BGs'].append({'BG':oneClassRow['BG']
								,'TotalStaySec':oneClassRow['TotalStaySec']
								,'TotalLearnPeople':oneClassRow['TotalLearnPeople']
								,'TotalLearn':oneClassRow['TotalLearn']
								,'AvgScore':0 if oneClassRow.get('AvgScore')is None else oneClassRow['AvgScore']
								,'PassPeople':0 if oneClassRow.get('PassPeople')is None else oneClassRow['PassPeople']
							})
					if find and oneClassRow['ClassId'] != LastClassObj3['ClassId'] :
						break
		resp = make_response(json.dumps(result ,ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
				
@restapi.resource('/iStudy/V1/PersonLearnInfo')
class PersonLearnInfo(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
		parser = reqparse.RequestParser()
		parser.add_argument('UserId', type=str)
		parser.add_argument('UserID', type=str)
		args = parser.parse_args()
		UserID = args['UserID'] if args["UserId"] is None else args["UserId"]
#		return {'hello': 'world'}
		cursor = mysql.connection.cursor(cursors.DictCursor)
		print(UserID)
		cursor.execute("call iStudy_View.SP_GetPersonCourseLearnInfo(%s);",[UserID])
		CourseInfoRows = cursor.fetchall()
		cursor.nextset()
		personInfo = cursor.fetchall()
		
		cursor.close()
		result = {"CourseInfos":[]}
		TotalCourse = 0
		SumStaySec = 0
		SumScore = 0
		PassCourses = 0
		SumLearnCount = 0
		LastClassObj1 = None
		LastClassObj2 = None
		LastClassObj3 = None
		LastCourseObj = None
		for oneCourseInfoRow in CourseInfoRows :
			if LastClassObj1 is None or LastClassObj1['ClassId'] != oneCourseInfoRow['ClassId1'] :
				LastClassObj1 = {"ClassId":oneCourseInfoRow["ClassId1"],"ClassName":oneCourseInfoRow["Class1"],"Classes":[]}
				LastClassObj2 = {"ClassId":oneCourseInfoRow["ClassId2"],"ClassName":oneCourseInfoRow["Class2"],"Classes":[]}
				LastClassObj3 = {"ClassId":oneCourseInfoRow["ClassId3"],"ClassName":oneCourseInfoRow["Class3"],"Courses":[]}
				result['CourseInfos'].append(LastClassObj1)
				LastClassObj1['Classes'].append(LastClassObj2)
				LastClassObj2['Classes'].append(LastClassObj3)
			if LastClassObj2['ClassId'] != oneCourseInfoRow["ClassId2"] :
				LastClassObj2 = {"ClassId":oneCourseInfoRow["ClassId2"],"ClassName":oneCourseInfoRow["Class2"],"Classes":[]}
				LastClassObj3 = {"ClassId":oneCourseInfoRow["ClassId3"],"ClassName":oneCourseInfoRow["Class3"],"Courses":[]}
				LastClassObj1['Classes'].append(LastClassObj2)
				LastClassObj2['Classes'].append(LastClassObj3)
			if LastClassObj3['ClassId'] != oneCourseInfoRow["ClassId3"] :
				LastClassObj3 = {"ClassId":oneCourseInfoRow["ClassId3"],"ClassName":oneCourseInfoRow["Class3"],"Courses":[]}
				LastClassObj2['Classes'].append(LastClassObj3)
			CourseObj = {"CourseId":oneCourseInfoRow["CourseId"]
				,"CourseName":oneCourseInfoRow["CourseName"]
				,"StaySec":oneCourseInfoRow["StaySec"]
				,"LearnCount":oneCourseInfoRow["LearnCount"]
				,"Score":0 if oneCourseInfoRow.get('Score') is None else oneCourseInfoRow['Score']
				,"IsPass":0 if oneCourseInfoRow.get('Pass') is None else oneCourseInfoRow['Pass']
			}
			TotalCourse += 1
			SumStaySec += CourseObj['StaySec']
			SumScore += CourseObj['Score']
			if CourseObj['IsPass'] == 1 :
				PassCourses += 1
			SumLearnCount += CourseObj['LearnCount']
			LastClassObj3['Courses'].append(CourseObj)
		if len(personInfo) > 0 :
			getcontext().prec = 2
			result["UserId"] = personInfo[0]["UserId"]
			result["Name"] = personInfo[0]["Name"]
			result["Sex"] = '女' if personInfo[0]["Sex"] == '0' else '男'
			result["Edu"] = personInfo[0]["Edu"]
			result["Area"] = personInfo[0]["Area"]
			result["Salary"] = personInfo[0]["Salary"]
			result["Age"] = personInfo[0]["Age"]
			result["JobTime"] = ceil(int(personInfo[0]["JobTime"])/52)
			result["RegisteredDate"] = datetime.strftime(personInfo[0]["RegisteredDate"],'%Y-%m-%d')
			result["BG"] = personInfo[0]["BG"]
			result["BU"] = personInfo[0]["BU"]
			result["Dept"] = personInfo[0]["DEPT"]
			result["TotalCourse"] = TotalCourse
			result["SumStaySec"] = SumStaySec
			result["TotalPassCourse"] = PassCourses
			result["SumLearnCount"] = SumLearnCount
			result["AvgScore"] = float(Decimal(SumScore/TotalCourse))
			result["AvgStaySec"] = float(Decimal(SumStaySec/TotalCourse))
		else :
			result["UserId"] = None
			result["Name"] = None
			result["Sex"] = None
			result["Edu"] = None
			result["Area"] = None
			result["Salary"] = None
			result["Age"] = 0
			result["JobTime"] = 0
			result["RegisteredDate"] = None
			result["BG"] = None
			result["BU"] = None
			result["Dept"] = None
			result["TotalCourse"] = 0
			result["SumStaySec"] = 0
			result["TotalPassCourse"] = 0
			result["SumLearnCount"] = 0
			result["AvgScore"] = 0
			result["AvgStaySec"] = 0
		
		resp = make_response(json.dumps(result ,ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp

@restapi.resource('/iStudy/V1/GroupLearnInfoByClass')
class GroupLearnInfoByClass(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
#		return {'hello': 'world'}
		cursor = mysql.connection.cursor(cursors.DictCursor)
		cursor.execute('Call iStudy_View.SP_GetGroupLearnInfo();')
		CourseInfos = cursor.fetchall()
		cursor.nextset()
		ClassInfos = cursor.fetchall()
		cursor.nextset()
		ClassInfos2 = cursor.fetchall()
		cursor.close()
		result = []
		lastGroup1 = None
		lastGroup2 = None
		lastClass1 = None
		lastClass2 = None
		lastClass3 = None
		for onerow in CourseInfos :
			findGroup1 = False
			findGroup2 = False
			findClass1 = False
			findClass2 = False
			findClass3 = False
			if lastGroup1 is None or lastGroup1['BG'] != onerow['Group1'] :
				lastGroup1 = {"BG":onerow['Group1'],"ClassInfos":[],"SubGroups":[]}
				lastGroup2 = {"BG":onerow['Group1'],"ClassInfos":[],"SubClasses":[]}
				lastClass1 = {"ClassId":onerow['ClassId1'],"ClassName":onerow["Class1"],"SubClasses":[]}
				lastClass2 = {"ClassId":onerow['ClassId2'],"ClassName":onerow["Class1"],"SubClasses":[]}
				lastClass3 = {"ClassId":onerow['ClassId3'],"ClassName":onerow["Class1"],"SubCourses":[]}
				result.append(lastGroup1)
				lastGroup1['SubGroups'].append(lastGroup2)
				lastGroup2['SubClasses'].append(lastClass1)
				lastClass1['SubClasses'].append(lastClass2)
				lastClass2['SubClasses'].append(lastClass3)
				findGroup1 = True
				findGroup2 = True
				findClass1 = True
				findClass2 = True
				findClass3 = True
			if lastGroup2['BG'] != onerow['Group2'] :
				lastGroup2 = {'BG':onerow['Group2'],'ClassInfos':[],'SubClasses':[]}
				lastClass1 = {"ClassId":onerow['ClassId1'],"ClassName":onerow["Class1"],"SubClasses":[]}
				lastClass2 = {"ClassId":onerow['ClassId2'],"ClassName":onerow["Class1"],"SubClasses":[]}
				lastClass3 = {"ClassId":onerow['ClassId3'],"ClassName":onerow["Class1"],"SubCourses":[]}
				lastGroup1['SubGroups'].append(lastGroup2)
				lastGroup2['SubClasses'].append(lastClass1)
				lastClass1['SubClasses'].append(lastClass2)
				lastClass2['SubClasses'].append(lastClass3)
				findGroup2 = True
				findClass1 = True
				findClass2 = True
				findClass3 = True
			if lastClass1['ClassId'] != onerow['ClassId1'] :
				lastClass1 = {"ClassId":onerow['ClassId1'],"ClassName":onerow["Class1"],"SubClasses":[]}
				lastClass2 = {"ClassId":onerow['ClassId2'],"ClassName":onerow["Class1"],"SubClasses":[]}
				lastClass3 = {"ClassId":onerow['ClassId3'],"ClassName":onerow["Class1"],"SubCourses":[]}
				lastGroup2['SubClasses'].append(lastClass1)
				lastClass1['SubClasses'].append(lastClass2)
				lastClass2['SubClasses'].append(lastClass3)
				findClass1 = True
				findClass2 = True
				findClass3 = True
			if lastClass2['ClassId'] != onerow['ClassId2'] :
				lastClass2 = {"ClassId":onerow['ClassId2'],"ClassName":onerow["Class1"],"SubClasses":[]}
				lastClass3 = {"ClassId":onerow['ClassId3'],"ClassName":onerow["Class1"],"SubCourses":[]}
				lastClass1['SubClasses'].append(lastClass2)
				lastClass2['SubClasses'].append(lastClass3)
				findClass2 = True
				findClass3 = True
			if lastClass3['ClassId'] != onerow['ClassId3'] :
				lastClass3 = {"ClassId":onerow['ClassId3'],"ClassName":onerow["Class1"],"SubCourses":[]}
				lastClass2['SubClasses'].append(lastClass3)
				findClass3 = True
			if findGroup1 :
				for oneClassInfoRow in ClassInfos :
					if oneClassInfoRow['BG'] == lastGroup1['BG'] :
						if oneClassInfoRow['ClassId1'] == 'All' : 
							lastGroup1['TotalStaySec'] = int(oneClassInfoRow['StaySec'])
							lastGroup1['TotalLearn'] = int(oneClassInfoRow['LearnCount'])
							lastGroup1['TotalLearnPeople'] = int(oneClassInfoRow['LearnPeople'])
							lastGroup1['AvgScore'] = 0 if oneClassInfoRow['AvgScore'] is None else float(oneClassInfoRow['AvgScore'])
							lastGroup1['PassPeople'] = 0 if oneClassInfoRow['PassPeople'] is None else int(oneClassInfoRow['PassPeople'])
							lastGroup1['PassRatio'] = 0 if oneClassInfoRow['PassRatio'] is None else float(oneClassInfoRow['PassRatio'])
						else :
							lastGroup1['ClassInfos'].append({'ClassId':oneClassInfoRow['ClassId1']
								,'ClassName':oneClassInfoRow['Class1']
								,'TotalStaySec':int(oneClassInfoRow['StaySec'])
								,'TotalLearn':int(oneClassInfoRow['LearnCount'])
								,'TotalLearnPeople':int(oneClassInfoRow['LearnPeople'])
								,'AvgScore': 0 if oneClassInfoRow['AvgScore'] is None else float(oneClassInfoRow['AvgScore'])
								,'PassPeople' : 0 if oneClassInfoRow['PassPeople'] is None else int(oneClassInfoRow['PassPeople'])
								,'PassRatio' : 0 if oneClassInfoRow['PassRatio'] is None else float(oneClassInfoRow['PassRatio'])})
			if findGroup2 :
				for oneClassInfoRow in ClassInfos :
					if oneClassInfoRow['BG'] == lastGroup2['BG'] : 
						if oneClassInfoRow['ClassId1'] == 'All' :
							lastGroup2['TotalStaySec'] = int(oneClassInfoRow['StaySec'])
							lastGroup2['TotalLearn'] = int(oneClassInfoRow['LearnCount'])
							lastGroup2['TotalLearnPeople'] = int(oneClassInfoRow['LearnPeople'])
							lastGroup2['AvgScore'] = 0 if oneClassInfoRow['AvgScore'] is None else float(oneClassInfoRow['AvgScore'])
							lastGroup2['PassPeople'] = 0 if oneClassInfoRow['PassPeople'] is None else int(oneClassInfoRow['PassPeople'])
							lastGroup2['PassRatio'] = 0 if oneClassInfoRow['PassRatio'] is None else float(oneClassInfoRow['PassRatio'])
						else :
							lastGroup2['ClassInfos'].append({'ClassId':oneClassInfoRow['ClassId1']
								,'ClassName':oneClassInfoRow['Class1']
								,'TotalStaySec':int(oneClassInfoRow['StaySec'])
								,'TotalLearn':int(oneClassInfoRow['LearnCount'])
								,'TotalLearnPeople':int(oneClassInfoRow['LearnPeople'])
								,'AvgScore': 0 if oneClassInfoRow['AvgScore'] is None else float(oneClassInfoRow['AvgScore'])
								,'PassPeople' : 0 if oneClassInfoRow['PassPeople'] is None else int(oneClassInfoRow['PassPeople'])
								,'PassRatio' : 0 if oneClassInfoRow['PassRatio'] is None else float(oneClassInfoRow['PassRatio'])})
			if findClass1 :
				for oneClassRow in ClassInfos2:
					if oneClassRow['Group1'] == lastGroup1['BG'] \
						and oneClassRow['Group2'] == lastGroup2['BG'] \
						and oneClassRow['ClassId'] == lastClass1['ClassId'] :
						lastClass1['TotalStaySec'] = int(oneClassRow['StaySec'])
						lastClass1['TotalLearn'] = int(oneClassRow['LearnCount'])
						lastClass1['TotalLearnPeople'] = int(oneClassRow['LearnPeople'])
						lastClass1['AvgScore'] = 0 if oneClassRow['AvgScore'] is None else float(oneClassRow['AvgScore'])
						lastClass1['PassPeople'] = 0 if oneClassRow['PassPeople'] is None else int(oneClassRow['PassPeople'])
						lastClass1['PassRatio'] = 0 if oneClassRow['PassRatio'] is None else float(oneClassRow['PassRatio'])
						break
			if findClass2 :
				for oneClassRow in ClassInfos2:
					if oneClassRow['Group1'] == lastGroup1['BG'] \
						and oneClassRow['Group2'] == lastGroup2['BG'] \
						and oneClassRow['ClassId'] == lastClass2['ClassId'] :
						lastClass2['TotalStaySec'] = int(oneClassRow['StaySec'])
						lastClass2['TotalLearn'] = int(oneClassRow['LearnCount'])
						lastClass2['TotalLearnPeople'] = int(oneClassRow['LearnPeople'])
						lastClass2['AvgScore'] = 0 if oneClassRow['AvgScore'] is None else float(oneClassRow['AvgScore'])
						lastClass2['PassPeople'] = 0 if oneClassRow['PassPeople'] is None else int(oneClassRow['PassPeople'])
						lastClass2['PassRatio'] = 0 if oneClassRow['PassRatio'] is None else float(oneClassRow['PassRatio'])
						break
			if findClass3 :
				for oneClassRow in ClassInfos2:
					if oneClassRow['Group1'] == lastGroup1['BG'] \
						and oneClassRow['Group2'] == lastGroup2['BG'] \
						and oneClassRow['ClassId'] == lastClass2['ClassId'] :
						lastClass3['TotalStaySec'] = int(oneClassRow['StaySec'])
						lastClass3['TotalLearn'] = int(oneClassRow['LearnCount'])
						lastClass3['TotalLearnPeople'] = int(oneClassRow['LearnPeople'])
						lastClass3['AvgScore'] = 0 if oneClassRow['AvgScore'] is None else float(oneClassRow['AvgScore'])
						lastClass3['PassPeople'] = 0 if oneClassRow['PassPeople'] is None else int(oneClassRow['PassPeople'])
						lastClass3['PassRatio'] = 0 if oneClassRow['PassRatio'] is None else float(oneClassRow['PassRatio'])
						break
			Course = {"CourseId":onerow['CourseId']
				,"CourseName":onerow['CourseName']
				,"TotalStaySec":int(onerow["StaySec"])
				,"TotalLearn":int(onerow["LearnCount"])
				,"TotalLearnPeople":int(onerow["LearnPeople"])
				,"AvgScore":0 if onerow['AvgScore'] is None else float(onerow['AvgScore'])
				,"PassPeople":0 if onerow['PassPeople'] is None else int(onerow['PassPeople'])
				,"PassRatio":0 if onerow['PassRatio'] is None else float(onerow['PassRatio'])
			}
			lastClass3['SubCourses'].append(Course)
		resp = make_response(json.dumps(result ,ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
@restapi.resource('/iStudy/V1/SummaryInfo')
class SummaryInfo(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
		cursor = mysql.connection.cursor(cursors.DictCursor)
		cursor.execute('Call iStudy_View.SP_GetSummaryInfo();')
		TotalUserRows = cursor.fetchall()
		cursor.nextset()
		ActiverUserRows = cursor.fetchall()
		cursor.nextset()
		TotalCourse = cursor.fetchall()
		cursor.nextset()
		StaySecRows = cursor.fetchall()
		cursor.nextset()
		PassRatioRows = cursor.fetchall()
		cursor.nextset()
		NewUserDayRows = cursor.fetchall()
		cursor.nextset()
		NewUserMonthRows = cursor.fetchall()
		cursor.close()
#		mysql.connection.close()
#		print(len(TotalUserRows))
#		print(len(ActiverUserRows))
#		print(len(TotalCourse))
#		print(len(StaySecRows))
#		print(len(PassRatioRows))
#		print(len(NewUserDayRows))
#		print(len(NewUserMonthRows))
#		print(TotalCourse)
		result = {"TotalUser":{"TotalUser":0,"SubGroups":[]}
		,"ActiveUser":{"ActiveUserCount":0,"UnactiveUserCount":0,"DormancyUserCount":0,"Dates":[]}
		,"TotalCourse":{"TotalCourse":int(TotalCourse[0]["TotalCourse"])}
		,"StaySec":{"SumStaySec":0,"AvgStaySec":0,"SubGroups":[]}
		,"PassRatio":{"PassRatio":0,"SubGroups":[]}
		,"NewUserSoonWeek":{"NewUser":0,"Dates":[]}
		,"NewUserSoonHalfYear":{"NewUser":0,"Months":[]}}
		
		for onerow in TotalUserRows :
			findGroup1 = None
			for oneResultRow in result['TotalUser']['SubGroups'] :
				if oneResultRow['BG'] == onerow['Group1'] :
					findGroup1 = oneResultRow
					break
			if onerow['Group1'] != 'All' and findGroup1 is None :
				findGroup1 = {"BG":onerow['Group1'],"SubGroups":[]}
				result['TotalUser']['SubGroups'].append(findGroup1)
			if onerow['Group1'] == 'All' and onerow['Group2'] == 'All' :
				result['TotalUser']['TotalUser'] = int(onerow['TotalUser'])
			elif onerow['Group2'] == 'All' :
				findGroup1['TotalUser'] = int(onerow['TotalUser'])
			else :
				findGroup1['SubGroups'].append({"BG":onerow["Group2"],"TotalUser":int(onerow["TotalUser"])})
				
		for onerow in ActiverUserRows :
			findDate = None
			for oneActiveUserRow in result['ActiveUser']['Dates'] :
				if oneActiveUserRow['Date'] == onerow['ActiveDate'] :
					findDate = oneActiveUserRow
					break
			if findDate is None :
				findDate = {"Date":onerow["ActiveDate"],"ActiveUserCount":0,"UnactiveUserCount":0,"DormancyUserCount":0,"SubGroups":[]}
				result["ActiveUser"]["Dates"].append(findDate)
			findGroup1 = None
			for oneGroup in findDate['SubGroups'] :
				if oneGroup['BG'] == onerow['Group1'] :
					findGroup1 = oneGroup
					break
#			print(findGroup1)
			if onerow['Group1'] != 'All' and findGroup1 is None :
				findGroup1 = {"BG":onerow['Group1'],"ActiveUserCount":0,"UnactiveUserCount":0,"DormancyUserCount":0,"SubGroups":[]}
				findDate["SubGroups"].append(findGroup1)
			if onerow['Group1'] == 'All' and onerow['Group2'] == 'All' :
				if onerow['LoginKind'] == '1' :
					findDate["ActiveUserCount"] = int(onerow["UserCount"])
				if onerow['LoginKind'] == '2' :
					findDate["UnactiveUserCount"] = int(onerow["UserCount"])
				if onerow['LoginKind'] == '12' :
					findDate["DormancyUserCount"] = int(onerow["UserCount"])
			elif onerow['Group1'] == 'All' :
				if onerow['LoginKind'] == '1' :
					findGroup1["ActiveUserCount"] = int(onerow["UserCount"])
				if onerow['LoginKind'] == '2' :
					findGroup1["UnactiveUserCount"] = int(onerow["UserCount"])
				if onerow['LoginKind'] == '12' :
					findGroup1["DormancyUserCount"] = int(onerow["UserCount"])
			else :
				findGroup2 = None
#				print(findGroup1)
				for oneGroup1Row in findGroup1["SubGroups"] :
					if oneGroup1Row["BG"] == onerow["Group2"] :
						findGroup2 = oneGroup1Row
						break
				if findGroup2 is None :
					findGroup2 = {"BG":onerow["Group2"],"ActiveUserCount":0,"UnactiveUserCount":0,"DormancyUserCount":0}
					findGroup1["SubGroups"].append(findGroup2)
				if onerow['LoginKind'] == '1' :
					findGroup2["ActiveUserCount"] = int(onerow["UserCount"])
				if onerow['LoginKind'] == '2' :
					findGroup2["UnactiveUserCount"] = int(onerow["UserCount"])
				if onerow['LoginKind'] == '12' :
					findGroup2["DormancyUserCount"] = int(onerow["UserCount"])
		for onerow in StaySecRows :
			findGroup1 = None
			for oneStaySecRow in result['StaySec']['SubGroups'] :
				if oneStaySecRow['BG'] == onerow['Group1'] :
					findGroup1 = oneStaySecRow
					break
			if onerow['Group1'] != 'All' and findGroup1 is None :
				findGroup1 = {"BG":onerow['Group1'],"SubGroups":[]}
				result["StaySec"]["SubGroups"].append(findGroup1)
			if onerow["Group1"] == 'All' and onerow["Group2"] == 'All':
				result["StaySec"]["SumStaySec"] = int(onerow["StaySec"])
				result["StaySec"]["AvgStaySec"] = float(onerow["AvgStaySec"])
			elif onerow["Group2"] == 'All':
				findGroup1["SumStaySec"] = int(onerow["StaySec"])
				findGroup1["AvgStaySec"] = float(onerow["AvgStaySec"])
			else :
				findGroup1["SubGroups"].append({"BG":onerow["Group2"],"SumStaySec":int(onerow["StaySec"]),"AvgStaySec":float(onerow["AvgStaySec"])})
		
		for onerow in PassRatioRows :
			findGroup1 = None
			for onePassRatioRow in result["PassRatio"]["SubGroups"] :
				if onePassRatioRow["BG"] == onerow["Group1"] :
					findGroup1 = onePassRatioRow
					break
			if onerow["Group1"] != 'All' and findGroup1 is None :
				findGroup1 = {"BG":onerow["Group1"],"SubGroups":[]}
				result["PassRatio"]["SubGroups"].append(findGroup1)
			if onerow["Group1"] == 'All' and onerow["Group2"] == 'All' :
				result["PassRatio"]["PassRatio"] = float(onerow["PassRatio"])
				result["PassRatio"]["PassPeople"] = int(onerow["PassPeople"])
				result["PassRatio"]["AvgScore"] = float(onerow["AvgScore"])
				result["PassRatio"]["LearnPeople"] = int(onerow["LearnPeople"])
			elif onerow["Group2"] == 'All' :
				findGroup1["PassRatio"] = float(onerow["PassRatio"])
				findGroup1["PassPeople"] = int(onerow["PassPeople"])
				findGroup1["AvgScore"] = float(onerow["AvgScore"])
				findGroup1["LearnPeople"] = int(onerow["LearnPeople"])
			else :
				findGroup1["SubGroups"].append({"BG":onerow["Group2"]
				,"PassRatio":float(onerow["PassRatio"])
				,"PassPeople":int(onerow["PassPeople"])
				,"AvgScore":float(onerow["AvgScore"])
				,"LearnPeople":int(onerow["LearnPeople"])})
		for onerow in NewUserDayRows :
			findDate = None
			for oneNewUserSoonWeekRow in result['NewUserSoonWeek']['Dates'] :
				if oneNewUserSoonWeekRow["Date"] == onerow["Day"] :
					findDate = oneNewUserSoonWeekRow
					break
			if findDate is None :
				findDate = {"Date":onerow["Day"],"NewUser":0,"SubGroups":[]}
				result["NewUserSoonWeek"]["Dates"].append(findDate)
			findGroup1 = None
			for oneDate in findDate["SubGroups"] :
				if oneDate["BG"] == onerow["Group1"] :
					findGroup1 = oneDate
					break
			if onerow["Group1"] != 'All' and findGroup1 is None :
				findGroup1 = {"BG":onerow["Group1"],"SubGroups":[]}
				findDate["SubGroups"].append(findGroup1)
			if onerow["Group1"] == 'All' and onerow["Group2"] == 'All' :
				findDate["NewUser"] = int(onerow["NewUser"])
			elif onerow["Group2"] == 'All':
				findGroup1["NewUser"] = int(onerow["NewUser"])
			else :
				findGroup1["SubGroups"].append({"BG":onerow["Group2"],"NewUser":int(onerow["NewUser"])})
		if len(result["NewUserSoonWeek"]["Dates"]) > 0 :
			result["NewUserSoonWeek"]["NewUser"] = result["NewUserSoonWeek"]["Dates"][0]["NewUser"]
		for onerow in NewUserMonthRows :
			findMonth = None
			for oneNewUserSoonHalfYearRow in result["NewUserSoonHalfYear"]["Months"] :
				if oneNewUserSoonHalfYearRow["Month"] == onerow["Month"] :
					findMonth = oneNewUserSoonHalfYearRow
					break
			if findMonth is None :
				findMonth = {"Month":onerow["Month"],"NewUser":0,"SubGroups":[]}
				result["NewUserSoonHalfYear"]["Months"].append(findMonth)
			findGroup1 = None
			for oneGroup in findMonth["SubGroups"] :
				if oneGroup["BG"] == onerow["Group1"] :
					findGroup1 = oneGroup
					break
			if onerow["Group1"] != 'All' and findGroup1 is None :
				findGroup1 = {"BG":onerow["Group1"],"SubGroups":[]}
				findMonth["SubGroups"].append(findGroup1)
			if onerow["Group1"] == 'All' and onerow["Group2"] == 'All' :
				findMonth["NewUser"] = int(onerow["NewUser"])
			elif onerow["Group2"] == 'All' :
				findGroup1["NewUser"] = int(onerow["NewUser"])
			else :
				findGroup1["SubGroups"].append({"BG":onerow["Group2"],"NewUser":int(onerow["NewUser"])})
		if len(result["NewUserSoonHalfYear"]["Months"]) > 0 :
			result["NewUserSoonHalfYear"]["NewUser"] = result["NewUserSoonHalfYear"]["Months"][0]["NewUser"]
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
#		with open('data.txt', 'w', encoding='utf8') as f:
#			outdata = json.dumps(result, ensure_ascii=False)
#			f.write(outdata)
		
		return resp
		
@restapi.resource('/iStudy/V1/DateLearnInfo')
class DateLearnInfo(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
#		return {'hello': 'world'}
		cursor = mysql.connection.cursor(cursors.DictCursor)
		cursor.execute('call iStudy_View.SP_GetDateLearnInfo();')
		ActiveUserRows = cursor.fetchall()
		cursor.nextset()
		NewUserRows = cursor.fetchall()
		cursor.close()
		result = []
		lastYearObj = None
		lastMonthObj = None
		lastDateObj = None
		for onerow in ActiveUserRows :
			if lastYearObj is None or lastYearObj['Year'] != onerow['Year'] :
				lastYearObj = {"Year":onerow["Year"],"Months":[]}
				lastMonthObj = {"Month":onerow["Month"],"Dates":[]}
				lastDateObj = {"Date":onerow["Date"],"SubGroups":[]
					,"ActiveUserCount":0
					,"UnactiveUserCount":0
					,"DormancyUserCount":0
					,"NewUser":0}
				result.append(lastYearObj)
				lastYearObj["Months"].append(lastMonthObj)
				lastMonthObj["Dates"].append(lastDateObj)
			if lastMonthObj["Month"] != onerow["Month"] :
				lastMonthObj = {"Month":onerow["Month"],"Dates":[]}
				lastDateObj = {"Date":onerow["Date"],"SubGroups":[]
					,"ActiveUserCount":0
					,"UnactiveUserCount":0
					,"DormancyUserCount":0
					,"NewUser":0}
				lastYearObj["Months"].append(lastMonthObj)
				lastMonthObj["Dates"].append(lastDateObj)
			if lastDateObj["Date"] != onerow["Date"] :
				lastDateObj = {"Date":onerow["Date"],"SubGroups":[]
					,"ActiveUserCount":0
					,"UnactiveUserCount":0
					,"DormancyUserCount":0
					,"NewUser":0}
				lastMonthObj["Dates"].append(lastDateObj)
			findGroup1 = None
			for oneGroup in lastDateObj["SubGroups"] :
				if oneGroup["BG"] == onerow["Group1"] : 
					findGroup1 = oneGroup
					break
			if onerow["Group1"] != 'All' and findGroup1 is None :
				findGroup1 = {"BG":onerow["Group1"]
				,"ActiveUserCount":0
				,"UnactiveUserCount":0
				,"DormancyUserCount":0
				,"NewUser":0
				,"SubGroups":[]}
				lastDateObj["SubGroups"].append(findGroup1)
			if onerow["Group1"] == 'All' and onerow["Group2"] == 'All' :
				lastDateObj["ActiveUserCount"] = 0 if onerow["ActiveUser"] is None else int(onerow["ActiveUser"])
				lastDateObj["UnactiveUserCount"] = 0 if onerow["UnactiveUser"] is None else int(onerow["UnactiveUser"])
				lastDateObj["DormancyUserCount"] = 0 if onerow["DormancyUser"] is None else int(onerow["DormancyUser"])
			elif onerow["Group2"] == 'All' :
				findGroup1["ActiveUserCount"] = 0 if onerow["ActiveUser"] is None else int(onerow["ActiveUser"])
				findGroup1["UnactiveUserCount"] = 0 if onerow["UnactiveUser"] is None else int(onerow["UnactiveUser"])
				findGroup1["DormancyUserCount"] = 0 if onerow["DormancyUser"] is None else int(onerow["DormancyUser"])
			else :
				findGroup1["SubGroups"].append({"BG":onerow["Group2"]
					,"ActiveUserCount":0 if onerow["ActiveUser"] is None else int(onerow["ActiveUser"])
					,"UnactiveUserCount":0 if onerow["UnactiveUser"] is None else int(onerow["UnactiveUser"])
					,"DormancyUserCount":0 if onerow["DormancyUser"] is None else int(onerow["DormancyUser"])
					,"NewUser":0
				})
		lastYearObj = None
		lastMonthObj = None
		lastDateObj = None
		for onerow in NewUserRows :
			findGroup1 = None
			if lastYearObj is None or lastYearObj["Year"] != onerow["Year"] :
				for oneYearObj in result :
					if oneYearObj["Year"] == onerow["Year"] :
						lastYearObj = oneYearObj
						break
			if lastMonthObj is None or lastMonthObj["Month"] != onerow["Month"] :
				for oneMonthObj in lastYearObj["Months"] :
					if oneMonthObj["Month"] == onerow["Month"] :
						lastMonthObj = oneMonthObj
						break
			if lastDateObj is None or lastDateObj["Date"] != onerow["Date"] :
				for oneDateObj in lastMonthObj["Dates"] :
					if oneDateObj["Date"] == onerow["Date"] :
						findDateObj = oneDateObj
						break
			for oneGroup in findDateObj["SubGroups"] :
				if oneGroup["BG"] == onerow["Group1"] :
					findGroup1 = oneGroup
					break
			if onerow["Group1"] != 'All' and findGroup1 is None :
				findGroup1 = {"BG":onerow["Group1"]
				,"ActiveUserCount":0
				,"UnactiveUserCount":0
				,"DormancyUserCount":0
				,"NewUser":0
				,"SubGroups":[]}
				findDateObj["SubGroups"].append(findGroup1)
			if onerow["Group1"] == 'All' and onerow["Group2"] == 'All' :
				findDateObj["NewUser"] = int(onerow["NewUser"])
			elif onerow["Group2"] == 'All':
				findGroup1["NewUser"] = int(onerow["NewUser"])
			else :
				findGroup2 = None
				for oneGroup in findGroup1["SubGroups"] :
					if oneGroup["BG"] == onerow["Group2"] :
						findGroup2 = oneGroup
						break
				if findGroup2 is None :
					findGroup2 = {"BG":onerow["Group2"]
					,"ActiveUserCount":0
					,"UnactiveUserCount":0
					,"DormancyUserCount":0
					,"NewUser":int(onerow["NewUser"])}
					findGroup1["SubGroups"].append(findGroup2)
				else :
					findGroup2["NewUser"] = int(onerow["NewUser"])
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
				
@restapi.resource('/iStudy/V1/QueryBG')
class QueryBG(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
		parser = reqparse.RequestParser()
		parser.add_argument('BG', type=str)
		args = parser.parse_args()
		BG = args['BG']
#		return {'hello': 'world'}
		cursor = mysql.connection.cursor(cursors.DictCursor)
		cursor.execute('Call iStudy_View.SP_GetBG(%s);',['All' if BG is None else BG])
		rows = cursor.fetchall()
		cursor.close()
		result = []
		for onerow in rows :
			result.append({"BG":onerow["BG"],"ParentBG":onerow["ParentBG"]})
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
		
@restapi.resource('/iStudy/V1/QueryClassCourse')
class QueryClassCourse(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
		parser = reqparse.RequestParser()
		parser.add_argument('ClassID', type=str)
		args = parser.parse_args()
		ClassID = args['ClassID']
#		return {'hello': 'world'}
		cursor = mysql.connection.cursor(cursors.DictCursor)
		cursor.execute('Call iStudy_View.SP_GetClassCourse(%s);',['All' if ClassID is None else ClassID])
		rows = cursor.fetchall()
		cursor.close()
		result = []
		for onerow in rows :
			result.append({"Kind":onerow["Kind"],"ID":onerow["ID"],"Name":onerow["Name"],"ClassID":onerow["ClassID"]})
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
		
@restapi.resource('/iStudy/V1/LearnInfoByDateAndGroup')
class LearnInfoByDateAndGroup(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
		parser = reqparse.RequestParser()
		parser.add_argument('Group', type=str,location='args',required=True)
		parser.add_argument('StartDate',type=lambda x: datetime.strptime(x,'%Y-%m-%d'))
		parser.add_argument('EndDate',type=lambda x: datetime.strptime(x,'%Y-%m-%d'))
		
		args = parser.parse_args()
		Group = args['Group']
		StartDate = args['StartDate']
		EndDate = args['EndDate']
#		return {'Group': Group}
#		return {'hello': 'world','Group':Group,'StartDate':StartDate,'EndDate':EndDate}
		cursor = mysql.connection.cursor(cursors.DictCursor)
		cursor.execute('Call iStudy_View.SP_GetLearnInfoByGroupAndDate(%s,%s,%s);',(Group,StartDate,EndDate))
		rows = cursor.fetchall()
		cursor.close()
		result = []
		for onerow in rows :
			result.append({"Date":onerow["Date"]
				,"TotalUser":int(onerow["TotalUser"])
				,"ActiveUserCount":int(onerow["ActiveUserCount"])
				,"DormancyUserCount":int(onerow["DormancyUserCount"])
				,"UnactiveUserCount":int(onerow["UnactiveUserCount"])
				,"NewUser":int(onerow["NewUser"])
				,"AndroidUserCount":int(onerow["AndroidUserCount"])
				,"IosUserCount":int(onerow["IosUserCount"])
				,"BothuseUserCount":int(onerow["BothuseUserCount"])
				,"BG":onerow["BG"]
			})
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
		
@restapi.resource('/iStudy/V1/LearnInfoByGroup')
class LearnInfoByGroup(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
		parser = reqparse.RequestParser()
		parser.add_argument('Group', type=str)
		args = parser.parse_args()
		Group = args['Group']
#		return {'hello': 'world'}
		cursor = mysql.connection.cursor(cursors.DictCursor)
		cursor.execute('Call iStudy_View.SP_GetLearnInfoByGroup(%s);',[Group])
		rows = cursor.fetchall()
		cursor.close()
		result = []
		for onerow in rows :
			result.append({"TotalStaySec":int(onerow["TotalStaySec"])
				,"AvgStaySec":float(onerow["AvgStaySec"])
				,"AvgScore":float(onerow["AvgScore"])
				,"PassPeople":int(onerow["PassPeople"])
				,"PassRatio":float(onerow["PassRatio"])
				,"TotalLearnPeople":int(onerow["TotalLearnPeople"])
				,"BG":onerow["BG"]
			})
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
		
@restapi.resource('/iStudy/V1/LearnInfoByCourseClass')
class LearnInfoByCourseClass(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
		parser = reqparse.RequestParser()
		parser.add_argument('Kind', type=str)
		parser.add_argument('ID', type=str)
		args = parser.parse_args()
		Kind = args['Kind']
		ID = args['ID']
#		return {'hello': 'world'}
		cursor = mysql.connection.cursor(cursors.DictCursor)
		cursor.execute('Call iStudy_View.SP_GetLearnInfoByCourseClass(%s,%s);',(Kind,ID))
		rows = cursor.fetchall()
		cursor.close()
		result = []
		for onerow in rows :
			result.append({"TotalStaySec":int(onerow["TotalStaySec"])
				,"AvgStaySec":float(onerow["AvgStaySec"])
				,"AvgScore":float(onerow["AvgScore"])
				,"PassPeople":int(onerow["PassPeople"])
				,"PassRatio":float(onerow["PassRatio"])
				,"TotalLearnPeople":int(onerow["TotalLearnPeople"])
				,"Kind":onerow["Kind"]
				,"ID":onerow["ID"]
				,"Name":onerow["Name"]
			})
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
		
@restapi.resource('/iStudy/V1/QueryUserLearnInfo')
class QueryUserLearnInfo(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
		parser = reqparse.RequestParser()
		parser.add_argument('UserId', type=str)
		parser.add_argument('UserID', type=str)
		args = parser.parse_args()
		UserID = args.get('UserID') if args.get('UserId') is None else args.get('UserId')
		#return {'hello': 'world'}
		cursor = mysql.connection.cursor(cursors.DictCursor)
		cursor.execute('Call iStudy_View.SP_GetUserLearnInfo(%s);',[UserID])
		rows = cursor.fetchall()
		cursor.close()
		result = []
		for onerow in rows :
			result.append({"CourseId":onerow["CourseId"]
			,"CourseName":onerow["CourseName"]
			,"TotalStaySec":int(onerow["TotalStaySec"])
			,"LearnCount":int(onerow["LearnCount"])
			,"Score":int(onerow["Score"])
			,"Pass":onerow["Pass"]
			,"ErrorCount":int(onerow["ErrorCount"])
			,"AvgScore_BG1":float(onerow["AvgScore_BG1"])
			,"AvgScore_BG2":float(onerow["AvgScore_BG2"])
			,"AvgScore_BG3":float(onerow["AvgScore_BG3"])
			,"Note":int(onerow["Note"])
			,"UserId":onerow["UserId"]})
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp
@restapi.resource('/iStudy/V1/QueryUserInfo')
class QueryUserInfo(Resource):
	@cache.cached(timeout=600, key_prefix=cache_key, unless=None)
	def get(self, headers=None):
		parser = reqparse.RequestParser()
		parser.add_argument('UserId', type=str)
		parser.add_argument('UserID', type=str)
		args = parser.parse_args()
		UserID = args.get('UserID') if args.get('UserId') is None else args.get('UserId')
#		return {'hello': UserID}
		cursor = mysql.connection.cursor(cursors.DictCursor)
		cursor.execute('Call iStudy_View.SP_GetUserInfo(%s);',[UserID])
		rows = cursor.fetchall()
		cursor.close()
		result = []
		for onerow in rows :
			result.append({"UserName":onerow["Name"]
			,"Sex":onerow["Sex"]
			,"Factory":onerow["Factory"]
			,"JobYears":int(onerow["JobTime"])
			,"Salary":onerow["Salary"]
			,"Age":onerow["Age"]
			,"Edu":onerow["Edu"]
			,"Major":onerow["Major"]
			,"BG1":onerow["BG1"]
			,"BG2":onerow["BG2"]
			,"BG3":onerow["BG3"]
			,"Pic":onerow["Pic"]
			,"UserId":onerow["UserId"]})
		resp = make_response(
				json.dumps(result ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp