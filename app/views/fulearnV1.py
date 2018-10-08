#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint, json, make_response
from flask_restful import Resource, Api, reqparse

from . import views_blueprint
from app.extensions import restapi,cache,mysql,mysql2
from app.utils import cache_key
from MySQLdb import cursors
import logging

@restapi.resource('/fulearn/V1')
class fulearnV1(Resource):
	def get(self):
		return {'hello':'fulearnV1-api'}

@restapi.resource('/fulearn/V1/ActiveUser')
class getActiveUserCount(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute("select LoginKind ,sum(UserCount) as n,cast(cast(Date as date) as char(10)) as datestr"
		+" from fulearn_2_view.Date_Group_ActiveUser"
		+" group by Date,LoginKind order by Date Desc;")
		result = []
		month = []
		year = []
		lastyearObj = {}
		lastmonthObj = {}
		lastdateObj = {}
		for data in cursor.fetchall():
			yearstr = data['datestr'][:4]
			monthstr = data['datestr'][5:7]
			if lastyearObj == {} or lastyearObj.get('Year') != yearstr:
				lastyearObj = {'Year':yearstr,'Months':[]}
				lastmonthObj = {'Month':monthstr,'Dates':[]}
				lastdateObj = {'Date':data['datestr'],'UnactiveUserCount':0,'ActiveUserCount':0,'DormancyUserCount':0}
				lastmonthObj['Dates'].append(lastdateObj)
				lastyearObj['Months'].append(lastmonthObj)
				result.append(lastyearObj)
			if lastmonthObj.get('Month') != monthstr:
				lastmonthObj = {'Month':monthstr,'Dates':[]}
				lastdateObj = {'Date':data['datestr'],'UnactiveUserCount':0,'ActiveUserCount':0,'DormancyUserCount':0}
				lastmonthObj['Dates'].append(lastdateObj)
				lastyearObj['Months'].append(lastmonthObj)
			if lastdateObj.get('Date') != data['datestr']:
				lastdateObj = {'Date':data['datestr'],'UnactiveUserCount':0,'ActiveUserCount':0,'DormancyUserCount':0};
				lastmonthObj['Dates'].append(lastdateObj)
			if data['LoginKind'] == 'Active':
				lastdateObj['ActiveUserCount'] = int(data['n'])
			elif data['LoginKind'] == 'Unactive':
				lastdateObj['UnactiveUserCount'] = int(data['n'])
			elif data['LoginKind'] == 'Dormancy':
				lastdateObj['DormancyUserCount'] = int(data['n'])
		cursor.close()
		conn.close()
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/DeviceUser')
class getDeviceUserCount(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute("select case when DeviceKind = 'Both' then '121' when DeviceKind = 'Android' then '1' else '2' end as DeviceKind,sum(UserCount) as n,cast(Date as char(10)) as datestr"
		+" from fulearn_2_view.Date_Group_DeviceUser"
		+" group by DeviceKind,Date order by Date desc;")
		result = []
		month = []
		year = []
		lastyearObj = {}
		lastmonthObj = {}
		lastdateObj = {}
		for data in cursor.fetchall():
			yearstr = data['datestr'][:4]
			monthstr = data['datestr'][5:7]
			if lastyearObj == {} or lastyearObj.get('Year') != yearstr:
				lastyearObj = {'Year':yearstr,'Months':[]}
				lastmonthObj = {'Month':monthstr,'Dates':[]}
				lastdateObj = {'Date':data['datestr'],'IosUserCount':0,'AndroidUserCount':0,'BothuseUserCount':0}
				lastmonthObj['Dates'].append(lastdateObj)
				lastyearObj['Months'].append(lastmonthObj)
				result.append(lastyearObj)
			if lastmonthObj.get('Month') != monthstr:
				lastmonthObj = {'Month':monthstr,'Dates':[]}
				lastdateObj = {'Date':data['datestr'],'IosUserCount':0,'AndroidUserCount':0,'BothuseUserCount':0}
				lastmonthObj['Dates'].append(lastdateObj)
				lastyearObj['Months'].append(lastmonthObj)
			if lastdateObj.get('Date') != data['datestr']:
				lastdateObj = {'Date':data['datestr'],'IosUserCount':0,'AndroidUserCount':0,'BothuseUserCount':0};
				lastmonthObj['Dates'].append(lastdateObj)
			if data['DeviceKind'] == '1':
				lastdateObj['AndroidUserCount'] = int(data['n'])
			elif data['DeviceKind'] == '2':
				lastdateObj['IosUserCount'] = int(data['n'])
			elif data['DeviceKind'] == '121':
				lastdateObj['BothuseUserCount'] = int(data['n'])
		cursor.close()
		conn.close()
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/ExamDiff')
class getExamDiff(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		conn = mysql.connection
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetCourseExamDifficulty();')
		result = []
		LastCourseID = 0
		LastCourseObj = {}
		CourseList = {}
		Course = cursor.fetchall()
		cursor.nextset()
		rows = cursor.fetchall()
		for CourseId,CourseName in Course:
			CourseList[CourseId] = CourseName
		for CourseId, ExamId, ExamTitle, GRADE in rows:
			if int(CourseId) != LastCourseID:
				LastCourseObj['CourseID'] = int(CourseId)
				LastCourseObj['CourseTitle'] = CourseList[int(CourseId)]
				LastCourseObj['Exams'] = []
				LastCourseID = int(CourseId)
				result.append(LastCourseObj)
			LastCourseObj['Exams'].append({'CourseID':int(CourseId),'ExamId':ExamId,'Title':ExamTitle,'Grade':float(GRADE)})
		cursor.close()
		conn.close()
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/UserCourseDiff')
class getUserCourseDiff(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		conn = mysql2.connect()
		cursor = conn.cursor()
		##資料表不存在
		cursor.execute("SELECT EMP_NO as UserID,EXAMTITLEID as CourseID,cast(Grade as decimal(3,2)) as Grade"
			+" FROM fulearn_2_view.RACE_STUDENT"
			+" where !ISNULL(GRADE)"
			+" ORDER by EMP_NO,EXAMTITLEID")
		rows = cursor.fetchall()
		result = []
		lastUserObj = {}
		CourseObj = {}
		cursor.close()
		conn.close()
		for row in rows :
			UserID = row['UserID']
			CourseID = row['CourseID']
			Grade = row['Grade']
			if lastUserObj == {} or lastyearObj.get('UserID') != UserID:
				lastUserObj = {'UserID':UserID,'Courses':[]}
				result.append(lastUserObj)
			CourseObj = {'CourseID':CourseID,'Grade':Grade}
			lastUserObj['Courses'].append(CourseObj)
		
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/CourseUserDiff')
class getCourseUserDiff(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		conn = mysql2.connect()
		cursor = conn.cursor()
		##資料表不存在
		cursor.execute("SELECT EMP_NO as UserID,EXAMTITLEID as CourseID,cast(Grade as decimal(3,2)) as Grade"
			+" FROM fulearn_2_view.RACE_STUDENT"
			+" where !ISNULL(GRADE)"
			+" ORDER by EXAMTITLEID,EMP_NO")
		result = []
		lastCourseObj = {}
		UserObj = {}
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			UserID = row['UserID']
			CourseID = row['CourseID']
			Grade = row['Grade']
			if lastUserObj == {} or lastyearObj.get('CourseID') != CourseID:
				lastCourseObj = {'CourseID':CourseID,'Users':[]}
				result.append(lastCourseObj)
			UserObj = {'UserID':UserID,'Grade':Grade}
			lastCourseObj['Users'].append(UserObj)
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/CourseAttract')
class getCourseAttract(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetCourseAttentionAdhesion();')
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			CourseID = row['CourseID']
			CourseName = row['CourseName']
			CourseKind = row['CourseKind']
			Adhesion = row['Adhesion']
			Attention = row['Attention']
			IsFramerCourse = row['IsFramerCourse']
			if CourseID < 10000000:
				i = 2
			else:
				i = 1
			result.append({
					'CourseID' : CourseID
					,'CourseName' : CourseName
					,'IsFramerCourse' : IsFramerCourse
					,'CourseKind':CourseKind
					,'CourseVersion': int(i)
					,'Adhesion':float(Adhesion)
					,'Attention':float(Attention)
				})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/SearchKeyword')
class getSearchKeyword(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute("select q.KeyWord,q.n as count, case c.CourseId when null then null else 1 end  as a\n"
		+"from fulearn_2_view.query q\n"
		+"left join fulearn_2.log_data_CourseList c on q.KeyWord = CourseName Or q.KeyWord = ChapterName \n"
		+"where q.n > 10\n"
		+"group by q.KeyWord,q.n,case c.CourseId when null then null else 1 end\n"
		+"order by n desc;")
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for onerow in rows :
			result.append({'KeyWord':onerow['KeyWord']
			,'Count':onerow['count']
			,'KeyWordIsCourseName': True if onerow['a'] == 1 else False})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/FunctionUseCount')
class getFunctionUseCount(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute("select UseKind,Count,SumStaySec from fulearn_2_view.UseKindView order by Count,SumStaySec;")
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		
		result = []
		for onerow in rows :
			result.append({'Function':onerow['UseKind']
			,'Count':onerow['Count']
			,'SumStaySec': onerow['SumStaySec']})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/RecommendCourse')
class getRecommendCourse(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('UserID', type=str)
		args = parser.parse_args()
		id = args['UserID']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetKechengRecommend(%s);',(id,))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for onerow in rows :
			result.append({'CourseId':onerow['CourseId']
			,'CourseName':onerow['CourseName']
			,'RecommendValue': onerow['RecommendValue']
			,'CourseKind':'书籍' if onerow['CourseVersion'] == 1 else '视频'
			})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/SummaryInfo')
class getSummaryInfo(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		conn = mysql.connection
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetSummaryInfo();')
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
		cou = TotalCourse[0]
		result = {'TotalUser':{'TotalUser':0,'SubGroups':[]}
			,'ActiveUser':{'ActiveUserCount':0,'UnactiveUserCount':0,'DormancyUserCount':0,'Dates':[]}
			,'TotalCourse':{'TotalCourse':int(cou[0])}
			,'StaySec':{'SumStaySec':0,'AvgStaySec':0,'SubGroups':[]}
			,'PassRatio':{'PassRatio':0,'SubGroups':[]}
			,'NewUserSoonWeek':{'NewUser':0,'Dates':[]}
			,'NewUserSoonHalfYear':{'NewUser':0,'Months':[]}
			}
		for Group1, Group2, TotalUser in TotalUserRows:
			findGroup1 = {}
			for j in range(0,len(result['TotalUser']['SubGroups'])):
				if result['TotalUser']['SubGroups'][j].get('BG') == Group1:
					findGroup1 = result['TotalUser']['SubGroups'][j]
					break
			if Group1 != 'All' and findGroup1 == {}:
				findGroup1 = {'BG':Group1,'SubGroups':[]}
				result['TotalUser']['SubGroups'].append(findGroup1)
			if Group1 == 'All' and Group2 == 'All':
				result['TotalUser']['TotalUser'] = int(TotalUser)
			elif Group2 == 'All':
				findGroup1['TotalUser'] = int(TotalUser)
			else:
				findGroup1['SubGroups'].append({'BG':Group2,'TotalUser':int(TotalUser)})
		for ActiveDate,Group1,Group2,LoginKind,UserCount in ActiverUserRows:
			findDate = {}
			for j in range(0,len(result['ActiveUser']['Dates'])):
				if result['ActiveUser']['Dates'][j].get('Date') == ActiveDate:
					findDate = result['ActiveUser']['Dates'][j]
					break
			if findDate == {}:
				findDate = {'Date':ActiveDate,'ActiveUserCount':0,'UnactiveUserCount':0,'DormancyUserCount':0,'SubGroups':[]}
				result['ActiveUser']['Dates'].append(findDate)
			findGroup1 = {}
			for j in range(0,len(findDate['SubGroups'])):
				if findDate['SubGroups'][j].get('BG') == Group1:
					findGroup1 = findDate['SubGroups'][j]
					break
			if Group1 != 'All' and findGroup1 == {}:
				findGroup1 = {'BG':Group1
					,'ActiveUserCount':0
					,'UnactiveUserCount':0
					,'DormancyUserCount':0
					,'SubGroups':[]}
				findDate['SubGroups'].append(findGroup1)
			if Group1 == 'All' and Group2 == 'All':
				if LoginKind == '1':
					findDate['ActiveUserCount'] = int(UserCount)
				if LoginKind == '2':
					findDate['UnactiveUserCount'] = int(UserCount)
				if LoginKind == '12':
					findDate['DormancyUserCount'] = int(UserCount)
			elif Group2 == 'All':
				if LoginKind == '1':
					findGroup1['ActiveUserCount'] = int(UserCount)
				if LoginKind == '2':
					findGroup1['UnactiveUserCount'] = int(UserCount)
				if LoginKind == '12':
					findGroup1['DormancyUserCount'] = int(UserCount)
			else:
				findGroup2 = {}
				for j in range(0,len(findGroup1['SubGroups'])):
					if findGroup1['SubGroups'][j].get('BG') == Group2:
						findGroup2 = findGroup1['SubGroups'][j]
					break
				if findGroup2 == {}:
					findGroup2 = {'BG':Group2,'ActiveUserCount':0,'UnactiveUserCount':0,'DormancyUserCount':0}
					findGroup1['SubGroups'].append(findGroup2)
				if LoginKind == '1':
					findGroup2['ActiveUserCount'] = int(UserCount)
				if LoginKind == '2':
					findGroup2['UnactiveUserCount'] = int(UserCount)
				if LoginKind == '12':
					findGroup2['DormancyUserCount'] = int(UserCount)
		if len(result['ActiveUser']['Dates']) > 0:
			result['ActiveUser']['ActiveUserCount'] = result['ActiveUser']['Dates'][0]['ActiveUserCount']
			result['ActiveUser']['UnactiveUserCount'] = result['ActiveUser']['Dates'][0]['UnactiveUserCount']
			result['ActiveUser']['DormancyUserCount'] = result['ActiveUser']['Dates'][0]['DormancyUserCount']	
		for Group1,Group2,StaySec,AvgStaySec in StaySecRows:
			findGroup1 = {}
			for j in range(0,len(result['StaySec']['SubGroups'])):
				if result['StaySec']['SubGroups'][j].get('BG') == Group1:
					findGroup1 = result['StaySec']['SubGroups'][j]
					break
			if Group1 != 'All' and findGroup1 == {}:
				findGroup1 = {'BG':Group1,'SubGroups':[]}
				result['StaySec']['SubGroups'].append(findGroup1)
			if Group1 == 'All' and Group2 == 'All':
				result['StaySec']['SumStaySec'] = float(StaySec)
				result['StaySec']['AvgStaySec'] = float(AvgStaySec)
			elif Group2 == 'All':
				findGroup1['SumStaySec'] = float(StaySec)
				findGroup1['AvgStaySec'] = float(AvgStaySec)
			else:
				findGroup1['SubGroups'].append({'BG':Group2,'SumStaySec':float(StaySec),'AvgStaySec':float(AvgStaySec)})
		for Group1,Group2,PassRatio,PassPeople,AvgScore,LearnPeople in PassRatioRows:
			findGroup1 = {}
			for j in range(0,len(result['PassRatio']['SubGroups'])):
				if result['PassRatio']['SubGroups'][j].get('BG') == Group1:
					findGroup1 = result['PassRatio']['SubGroups'][j]
					break
			if Group1 != 'All' and findGroup1 == {}:
				findGroup1 = {'BG':Group1,'SubGroups':[]}
				result['PassRatio']['SubGroups'].append(findGroup1)
			if Group1 == 'All' and Group2 == 'All':
				result['PassRatio']['PassRatio'] = int(PassRatio)
				result['PassRatio']['PassPeople'] = int(PassPeople)
				result['PassRatio']['AvgScore'] = float(AvgScore)
				result['PassRatio']['LearnPeople'] = int(LearnPeople)
			elif Group2 == 'All':
				findGroup1['PassRatio'] = int(PassRatio)
				findGroup1['PassPeople'] = int(PassPeople)
				findGroup1['AvgScore'] = float(AvgScore)
				findGroup1['LearnPeople'] = int(LearnPeople)
			else:
				findGroup1['SubGroups'].append({'BG':Group2
					,'PassRatio':int(PassRatio)
					,'PassPeople':int(PassPeople)
					,'AvgScore':float(AvgScore)
					,'LearnPeople':int(LearnPeople)})
		for Day,Group1,Group2,NewUser in NewUserDayRows:
			findDate = {}
			for j in range(0,len(result['NewUserSoonWeek']['Dates'])):
				if result['NewUserSoonWeek']['Dates'][j].get('Date') == Day:
					findGroup1 = result['NewUserSoonWeek']['Dates'][j]
					break
			if findDate == {}:
				findDate = {'Date':Day,'NewUser':0,'SubGroups':[]}
				result['NewUserSoonWeek']['Dates'].append(findDate)
			findGroup1 = {}
			for j in range(0,len(findDate['SubGroups'])):
				if findDate['SubGroups'][j].get('BG') == Group1:
					findGroup1 = findDate['SubGroups'][j]
					break
			if Group1 != 'All' and findGroup1 == {}:
				findGroup1 = {'BG':Group1,'SubGroups':[]}
				findDate['SubGroups'].append(findGroup1)
			if Group1 == 'All' and Group2 == 'All':
				findDate['NewUser'] = int(NewUser)
			elif Group2 == 'All':
				findGroup1['NewUser'] = int(NewUser)
			else:
				findGroup1['SubGroups'].append({'BG':Group2,'NewUser':int(NewUser)})
		if len(result['NewUserSoonWeek']['Dates']) > 0:
			result['NewUserSoonWeek']['NewUser'] = result['NewUserSoonWeek']['Dates'][0]['NewUser']
		for Month,Group1,Group2,NewUser in NewUserMonthRows:
			findMonth = {}
			for j in range(0,len(result['NewUserSoonHalfYear']['Months'])):
				if result['NewUserSoonHalfYear']['Months'][j].get('Month') == Month:
					findGroup1 = result['NewUserSoonHalfYear']['Months'][j]
					break
			if findMonth == {}:
				findMonth = {'Month':Month,'NewUser':0,'SubGroups':[]}
				result['NewUserSoonHalfYear']['Months'].append(findMonth)
			findGroup1 = {}
			for j in range(0,len(findMonth['SubGroups'])):
				if findMonth['SubGroups'][j].get('BG') == Group1:
					findGroup1 = findMonth['SubGroups'][j]
					break
			if Group1 != 'All' and findGroup1 == {}:
				findGroup1 = {'BG':Group1,'SubGroups':[]}
				findMonth['SubGroups'].append(findGroup1)
			if Group1 == 'All' and Group2 == 'All':
				findMonth['NewUser'] = int(NewUser)
			elif Group2 == 'All':
				findGroup1['NewUser'] = int(NewUser)
			else:
				findGroup1['SubGroups'].append({'BG':Group2,'NewUser':int(NewUser)})
		if len(result['NewUserSoonHalfYear']['Months']) > 0:
			result['NewUserSoonHalfYear']['NewUser'] = result['NewUserSoonHalfYear']['Months'][0]['NewUser']
		cursor.close()
		conn.close()
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/person/')
class getPersonInfo(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('id', type=str)
		args = parser.parse_args()
		id = args['id']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('select A.UserName, A.NickName, B.NAME, B.Gender, B.EducationType,' +
				   'B.School, B.BG, B.BU, B.DEPT, B.AREA, B.BirthDay ' +
				   'FROM fulearn_2.personinfo A ' +
				   'LEFT JOIN fulearn_2.person B ON A.UserName = B.ID WHERE A.UserName = \''+id+'\';')
		result = {}
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result['id'] = row['UserName']
			result['nickName'] = row['NickName']
			result['name'] = row['NAME']
			result['gender'] = row['Gender']
			result['education'] = row['EducationType']
			result['school'] = row['School']
			result['bg'] = row['BG']
			result['bu'] = row['BU']
			result['dept'] = row['DEPT']
			result['area'] = row['AREA']
			result['birthday'] = str(row['BirthDay'])
			result['pic'] = "http://iedu.foxconn.com:8080/head_pic/{0}.jpg".format(row['UserName'])
		
		# for UserName, NickName, NAME, Gender, EducationType, School, BG, BU, DEPT, AREA, BirthDay in cursor.fetchall():
			# result['id'] = UserName
			# result['nickName'] = NickName
			# result['name'] = NAME
			# result['gender'] = Gender
			# result['education'] = EducationType
			# result['school'] = School
			# result['bg'] = BG
			# result['bu'] = BU
			# result['dept'] = DEPT
			# result['area'] = AREA
			# result['birthday'] = str(BirthDay)
			# result['pic'] = 'http://10.248.167.198/BigDataCenter/NMG/personal-dashboard/personPic.jpg'
		# cursor.close()
		# conn.close()
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/QueryBG')
class getQueryBG(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('BG', type=str)
		args = parser.parse_args()
		BG = args['BG']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetBG(%s);',(BG,))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
			'BG':row['BG']
			,'ParentBG':row['ParentBG']
			})

		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/QueryClassCourse')
class getQueryClassCourse(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('ClassID', type=str)
		args = parser.parse_args()
		ClassID = args['ClassID']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetClassCourse(%s);',(ClassID,))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({'Kind':row['Kind'],'ID':row['ID'],'Name':row['Name'],'ClassID':row['ClassID']})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/LearnInfoByDateAndGroup')
class getLearnInfoByDateAndGroup(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('Group', type=str,location='args',required=True)
		parser.add_argument('StartDate',type=str,location='args',required=True)
		parser.add_argument('EndDate',type=str,location='args',required=True)
		args = parser.parse_args()
		Group = args['Group']
		StartDate = args['StartDate']
		EndDate = args['EndDate']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetLearnInfoByGroupAndDate(%s,%s,%s);',(Group,StartDate,EndDate))
		result = []
		rows = cursor.fetchall()
		
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
			'Date':row['Date']
			,'TotalUser':row['TotalUser']
			,'ActiveUserCount':row['ActiveUserCount']
			,'DormancyUserCount':row['DormancyUserCount']
			,'UnactiveUserCount':row['UnactiveUserCount']
			,'NewUser':row['NewUser']
			,'AndroidUserCount':row['AndroidUserCount']
			,'IosUserCount':row['IosUserCount']
			,'BothuseUserCount':row['BothuseUserCount']
			,'NotSeriousUserCount':row['NotSeriousUserCount']
			,'AnticlimaxUserCount':row['AnticlimaxUserCount']
			,'IndecisiveUserCount':row['IndecisiveUserCount']
			,'UnswervingUserCount':row['UnswervingUserCount']
			,'BG':row['BG']
		})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/LearnInfoByMonthAndGroup')
class getLearnInfoByMonthAndGroup(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('Group', type=str,location='args',required=True)
		parser.add_argument('StartDate',type=str,location='args',required=True)
		parser.add_argument('EndDate',type=str,location='args',required=True)
		args = parser.parse_args()
		Group = args['Group']
		StartDate = args['StartDate']
		EndDate = args['EndDate']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetLearnInfoByGroupAndMonth(%s,%s,%s);',(Group,StartDate,EndDate))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
			'Date':row['Date']
			,'ActiveUserCount':row['ActiveUserCount']
			,'DormancyUserCount':row['DormancyUserCount']
			,'UnactiveUserCount':row['UnactiveUserCount']
			,'AndroidUserCount':row['AndroidUserCount']
			,'IosUserCount':row['IosUserCount']
			,'BothuseUserCount':row['BothuseUserCount']
			,'NotSeriousUserCount':row['NotSeriousUserCount']
			,'AnticlimaxUserCount':row['AnticlimaxUserCount']
			,'IndecisiveUserCount':row['IndecisiveUserCount']
			,'UnswervingUserCount':row['UnswervingUserCount']
			,'BG':row['BG']
			})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/LearnInfoByGroup')
class getLearnInfoByGroup(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('Group', type=str)
		args = parser.parse_args()
		Group = args['Group']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetLearnInfoByGroup(%s);',(Group,))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
				'TotalStaySec':row['TotalStaySec']
				,'AvgStaySec':row['AvgStaySec']
				,'AvgScore':row['AvgScore']
				,'PassPeople':row['PassPeople']
				,'PassRatio':row['PassRatio']
				,'TotalLearnPeople':row['TotalLearnPeople']
				,'BG':row['BG']
			})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/LearnInfoByCourseClass')
class getLearnInfoByCourseClass(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('Kind', type=str,location='args',required=True)
		parser.add_argument('ID',type=str,location='args',required=True)
		args = parser.parse_args()
		Kind = args['Kind']
		ID = args['ID']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetLearnInfoByCourseClass(%s,%s);',(Kind,ID))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
				'TotalStaySec':row['TotalStaySec']
				,'AvgStaySec':row['AvgStaySec']
				,'AvgScore':row['AvgScore']
				,'PassPeople':row['PassPeople']
				,'PassRatio':row['PassRatio']
				,'RightRatio':row['RightRatio']
				,'AnswerPeople':row['AnswerPeople']
				,'Difference':row['Difference']
				,'TotalLearnPeople':row['TotalLearnPeople']
				,'Kind':row['Kind']
				,'ID':row['ID']
				,'Name':row['Name']
			})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/LearnInfo/CourseId/<string:CourseId>')
class getLearnInfoByCourseClassUseCourse(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self,CourseId):
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetLearnInfoByCourseClass(%s,%s);',('Course',CourseId))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
				'TotalStaySec':row['TotalStaySec']
				,'AvgStaySec':row['AvgStaySec']
				,'AvgScore':row['AvgScore']
				,'PassPeople':row['PassPeople']
				,'RightRatio':row['RightRatio']
				,'AnswerPeople':row['AnswerPeople']
				,'Difference':row['Difference']
				,'TotalLearnPeople':row['TotalLearnPeople']
				,'Kind':row['Kind']
				,'ID':row['ID']
				,'Name':row['Name']
			})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/LearnInfo/CourseId/')
class getUserLearnInfoByCourse(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('CourseId', type=str)
		args = parser.parse_args()
		CourseId = args['CourseId']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetUserLearnInfoByCourse(%s);',(CourseId,))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
				'UserId':row['UserId']
				,'Name':row['Name']
				,'Sex':row['Sex']
				,'Edu':row['Edu']
				,'Area':row['Area']
				,'Salary':row['Salary']
				,'Age':row['Age']
				,'JobTime':row['JobTime']
				,'RegisteredDate':row['RegisteredDate']
				,'IsEmployee':row['IsEmployee']
				,'ErrCount':row['ErrorCount']
				,'Focus':row['Focus']
				,'RightRatio':row['RightRatio']
				,'Involved':row['Involved']
				,'Score':row['Score']
				,'Pass':row['Pass']
				,'TotalStaySec':row['TotalStaySec']
				,'LearnCount':row['LearnCount']
				,'CourseId':row['CourseId']
			})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/LearnInfo/Course/<string:CourseId>/StartDate/<string:StartDate>/EndDate/<string:EndDate>')
class getLearnInfoByCourseAndDate(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self,CourseId,StartDate,EndDate):
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetLearnInfoByCourseAndDate(%s,%s,%s);',(CourseId,StartDate,EndDate))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
				'Date':row['Date']
				,'Name':row['CourseName']
				,'CourseId':row['CourseId']
				,'NotSeriousUserCount':row['NotSerious']
				,'AnticlimaxUserCount':row['Anticlimax']
				,'IndecisiveUserCount':row['Indecisive']
				,'UnswervingUserCount':row['Unswerving']
			})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/LearnInfo/Month/Course/<string:CourseId>/StartDate/<string:StartDate>/EndDate/<string:EndDate>')
class getLearnInfoByCourseAndMonth(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self,CourseId,StartDate,EndDate):
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetLearnInfoByCourseAndMonth(%s,%s,%s);',(CourseId,StartDate,EndDate))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
					'Date':row['Date']
					,'Name':row['CourseName']
					,'CourseId':row['CourseId']
					,'NotSeriousUserCount':row['NotSerious']
					,'AnticlimaxUserCount':row['Anticlimax']
					,'IndecisiveUserCount':row['Indecisive']
					,'UnswervingUserCount':row['Unswerving']
				})
			
		
		
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/QueryUserLearnInfo')
class getQueryUserLearnInfo(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('UserId', type=str)
		args = parser.parse_args()
		UserId = args['UserId']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetUserLearnInfo(%s);',(UserId))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
				'CourseId':row['CourseId']
				,'CourseName':row['CourseName']
				,'TotalStaySec':row['TotalStaySec']
				,'LearnCount':row['LearnCount']
				,'Score':row['Score']
				,'Pass':row['Pass']
				,'ErrorCount':row['ErrorCount']
				,'Focus':row['Focus']
				,'Involved':row['Involved']
				,'AvgInvolved':row['AvgInvolved']
				,'AvgScore_BG1':row['AvgScore_BG1']
				,'AvgScore_BG2':row['AvgScore_BG2']
				,'AvgScore_BG3':row['AvgScore_BG3']
				,'Note':row['Note']
				,'UserId':row['UserId']
			})
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/QueryUserInfo')
class getQueryUserInfo(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('UserId', type=str)
		args = parser.parse_args()
		UserId = args['UserId']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetUserInfo(%s);',(UserId))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
				'UserName':row['Name']
				,'Sex':row['Sex']
				,'Factory':row['Factory']
				,'JobYears':row['JobTime']
				,'Salary':row['Salary']
				,'Age':row['Age']
				,'Edu':row['Edu']
				,'Major':row['Major']
				,'BG1':row['BG1']
				,'BG2':row['BG2']
				,'BG3':row['BG3']
				,'Pic':row['Pic']
				,'UserId':row['UserId']
			})
			
		
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/ExamLearnInfoByCourse')
class getExamLearnInfoByCourse(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('CourseId', type=str)
		args = parser.parse_args()
		CourseId = args['CourseId']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetExamLearnInfoByCourse(%s);',(CourseId))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
				'QuestionId':row['QuestionId']
				,'Title':row['Title']
				,'Focus':row['Focus']
				,'AnswerPeople':row['AnswerPeople']
				,'RightRatio':row['RightRatio']
				,'Identification':row['Identification']
				,'Difficulty':row['Difficulty']
				,'CourseID':row['CourseId']
			})
		
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/LearnInfoByUserIDAndDate')
class getLearnInfoByUserIdAndDate(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self,):
		parser = reqparse.RequestParser()
		parser.add_argument('UserId', type=str,location='args',required=True)
		parser.add_argument('StartDate',type=str,location='args',required=True)
		parser.add_argument('EndDate',type=str,location='args',required=True)
		args = parser.parse_args()
		UserId = args['UserId']
		StartDate = args['StartDate']
		EndDate = args['EndDate']
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetLearnInfoByUserIDAndDate(%s,%s,%s);',(UserId,StartDate,EndDate))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
				'UserId':row['UserId']
				,'Date':row['Date']
				,'TotalStaySec':row['TotalStaySec']
				,'AlertLevel':row['AlertLevel']
			})
		
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/RadarAbilityByUserId/<string:UserId>/ClassId/<string:ClassId>')
class getRadarAbilityByUserId(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self,UserId,ClassId):
		conn = mysql2.connect()
		cursor = conn.cursor()
		cursor.execute('call fulearn_2_view.SP_GetRadarAbilityByUserId(%s,%s);',(UserId,ClassId))
		result = []
		rows = cursor.fetchall()
		cursor.close()
		conn.close()
		for row in rows :
			result.append({
				'UserId':row['UserId']
				,'RadarClassId':row['RadarClassId']
				,'RadarClassName':row['RadarClassName']
				,'AbilityName':row['AbilityName']
				,'AbilityValue':row['AbilityValue']
			})
		
		return make_response(json.dumps(result,ensure_ascii=False))

@restapi.resource('/fulearn/V1/RadarAbilityByUserId')
class getRadarAbilityByUserIdUseArgs(Resource):
	@cache.cached(timeout=36000, key_prefix=cache_key, unless=None)
	def get(self):
		parser = reqparse.RequestParser()
		parser.add_argument('UserId', type=str)
		parser.add_argument('ClassId', type=str)
		args = parser.parse_args()
		UserId = args['UserId']
		ClassId = args['ClassId']
		if not UserId is None and not ClassId is None :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute('call fulearn_2_view.SP_GetRadarAbilityByUserId(%s,%s);',(UserId,ClassId))
			result = []
			rows = cursor.fetchall()
			cursor.close()
			conn.close()
			for row in rows :
				result.append({
					'UserId':row['UserId']
					,'RadarClassId':row['RadarClassId']
					,'RadarClassName':row['RadarClassName']
					,'AbilityName':row['AbilityName']
					,'AbilityValue':row['AbilityValue']
				})
			return make_response(json.dumps(result,ensure_ascii=False))
		else :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute('call fulearn_2_view.SP_GetRadarAbilityClassList(%s);',(UserId))
			result = []
			rows = cursor.fetchall()
			cursor.close()
			conn.close()
			for row in rows :
				result.append({
					'RadarClassId':row['RadarClassId']
					,'RadarClassName':row['RadarClassName']
					,'UserId':row['UserId']
				})
			
			return make_response(json.dumps(result,ensure_ascii=False))
