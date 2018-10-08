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
from datetime import date,datetime as dt, timedelta
import calendar  # for getting month name

@restapi.resource('/fulearn/V4/Department/Ability')
class FulearnV4DepartmentAbility(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default value of groups
        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'

        # store result
        result = []

        # updata info by user input
        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        ## The sql will return at least one row
        # 部門能力指標
        sql_deabIndex = '''
        SELECT * FROM fulearn_4_view.Department_by_DeabIndex
        WHERE User_Group1 = "{0}"
        AND User_Group2 = "{1}"
        AND User_Group3 = "{2}"
        '''.format(group1, group2, group3)

        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql_deabIndex)
            rows = cursor.fetchall()

            if cursor.rowcount>0:
                # there are data
                for row in rows:
                    result.append({
                        'group1': group1,
                        'group2': group2,
                        'group3': group3,
                        'ability': row['deabindex'] or -1,
                        'rank': row['derank'] or -1
                    })
        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/Ability failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()


        response = jsonify(result)
        response.status_code=200

        return result

# 部門學習概況
@restapi.resource('/fulearn/V4/Department/Statistic')
class FulearnV4DepartmentStatistic(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        # default value of groups
        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'

        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        # store result
        result = []

        # 學習概況 > 各部門人數
        sql = '''SELECT * FROM fulearn_4_view.Department_by_personCount
        WHERE Group1="{0}" AND Group2="{1}" AND Group3="{2}"
        '''.format(group1, group2, group3)

        #print(sql)

        # 學習概況 > 各部門課程數
        sql2 ='''
        SELECT * FROM fulearn_4_view.Department_by_sumOfCourse
        WHERE User_Group1 = "{0}" AND User_Group2="{1}" AND User_Group3="{2}"
        '''.format(group1, group2, group3)

        # 學習概況 > 各部門學分總數
        sql3 = '''
        SELECT * FROM fulearn_4_view.Department_by_sumOfScore
        WHERE User_Group1 = "{0}" AND User_Group2="{1}" AND User_Group3="{2}"
        '''.format(group1, group2, group3)


        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            # sql
            cursor.execute(sql)
            rows = cursor.fetchall()

            ## if there are results
            if cursor.rowcount>0:
                for r in rows:
                    result.append({
                        'group1': r['Group1'],
                        'group2': r['Group2'],
                        'group3': r['Group3'],
                        'peopleCount': r['students']
                    })
            else:
                result.append({
                    'group1': group1,
                    'group2': group2,
                    'group3': group3,
                    'peopleCount': 0
                })


            # sql2
            cursor.execute(sql2)
            rows = cursor.fetchall()

            if cursor.rowcount>0:
                for r in rows:
                    result[0]['courseCount']= r['sum']
            else:
                result[0]['courseCount'] = 0


            # sql3
            cursor.execute(sql3)
            rows = cursor.fetchall()

            if cursor.rowcount>0:
                for r in rows:
                    result[0]['score']=r['sumofscore']
            else:
                result[0]['score']= 0


        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/Statistic failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        return result

# 各部門月學時
@restapi.resource('/fulearn/V4/Department/MonthLearnTime')
class FulearnV4DepartmentLearnTime(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        # default values
        year = '2017'
        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'

        # store result
        result = []

        # update parameters
        if 'year' in request.args:
            year = request.args['year']

        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        sql=''' SELECT * FROM fulearn_4_view.Department_by_stuTimeMon
        WHERE User_Group1 = "{0}"
        AND User_Group2 = "{1}"
        AND User_Group3 = "{2}"
        AND year="{3}"
        '''.format(group1, group2, group3, year)

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
                        'group1': r['User_Group1'],
                        'group2': r['User_Group2'],
                        'group3': r['User_Group3'],
                        'month': r['month'],
                        'year':r['year'],
                        'studyTime': r['studytime']
                    })
            else:
                pass
                # datetime.date.today().month
                # today = dt.today()

                # for i in range(1,today.month+1):
                #     result.append({
                #         'group1': group1,
                #         'group2': group2,
                #         'group3': group3,
                #         'month': calendar.month_abbr[i],
                #         'year': year,
                #         'studyTime': -1
                #     })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/LearnTime failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200

        return result


@restapi.resource('/fulearn/V4/Department/CourseLearnTime')
class FulearnV4DepartmentCourseLearnTime(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default values
        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'
        data_length = 10

        # store result
        result = []

        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        if 'dataLength' in request.args:
            data_length = request.args['dataLength']

        sql='''SELECT * FROM fulearn_4_view.Department_by_avgTime as dv
        LEFT JOIN fulearn_4.CourseNameList as fc ON dv.CourseId=fc.CourseId
        WHERE User_Group1="{0}" AND User_Group2="{1}" AND User_Group3="{2}"
        GROUP BY dv.CourseId LIMIT {3}
        '''.format(group1, group2, group3, data_length)
        print(sql)

        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            # excute sql
            cursor.execute(sql)
            rows = cursor.fetchall()

            if cursor.rowcount>0:
                for r in rows:
                    result.append({
                        'courseName': r['CourseName'],
                        'learnTime': r['avgtime'],
                        'group1': r['User_Group1'],
                        'group2': r['User_Group2'],
                        'group3': r['User_Group3']
                    })
            else:
                result = []

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/CourseLearnTime failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200

        return result

# 已學課程>部門通過率
@restapi.resource('/fulearn/V4/Department/PassRate')
class FulearnV4DepartmentPassRate(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        # default value of groups
        group1 = '總部週邊'
        group2 = 'IE總處'
        group3 = '資訊科技'
        data_length = 10

         # updata info by user input
        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        # store result
        result = []

        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        sql = '''SELECT * FROM fulearn_4_view.Department_by_passRate as dbp
        LEFT JOIN fulearn_4.CourseNameList as fc ON dbp.CourseId=fc.CourseId
        WHERE User_Group1="{0}"
        AND User_Group2="{1}"
        AND User_Group3="{2}"
        LIMIT {3}
        '''.format(group1,group2, group3, data_length)

        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            # excute sql
            cursor.execute(sql)
            rows = cursor.fetchall()

            if cursor.rowcount>0:
                for r in rows:
                    result.append({
                        'courseName': r['CourseName'],
                        'passRate':r['prate'],
                        'group1': r['User_Group1'],
                        'group2': r['User_Group2'],
                        'group3': r['User_Group3']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/PassRate failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200

        return result

@restapi.resource('/fulearn/V4/Department/AvgScoreRank')
class FulearnV4DepartmentAvgScoreRank(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # 已學課程相關統計

        # default value of groups
        group1 = '總部週邊'
        group2 = 'IE總處'
        group3 = '資訊科技'

        # updata info by user input
        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        # store result
        result = []

        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        # SELECT * FROM `Department_by_agvScore` as d
        # LEFT JOIN fulearn_4.CourseNameList as f
        # ON d.CourseId = f.CourseId
        # WHERE User_Group1="iDPBG" AND User_Group2="CAA 商貿事業處" AND User_Group3="營銷及電商"
        sql = '''SELECT * FROM fulearn_4_view.Department_by_agvScore as dbs
        LEFT JOIN fulearn_4.CourseNameList as cnl
        ON dbs.CourseId = cnl.CourseId
        WHERE User_Group1="{0}"
        AND User_Group2="{1}"
        AND User_Group3="{2}"
        ORDER BY agvscore ASC
        '''.format(group1, group2, group3)

        try:
            conn = mysql2.connect()
            cursor = conn.cursor()

            # excute sql
            cursor.execute(sql)
            rows = cursor.fetchall()

            if cursor.rowcount>0:
                for r in rows:
                    result.append({
                        'courseName': r['CourseName'],
                        'courseId':r['CourseId'],
                        'avgScore':r['agvscore'],
                        'group1': r['User_Group1'],
                        'group2': r['User_Group2'],
                        'group3': r['User_Group3']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/PassRate failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()


        response = jsonify(result)
        response.status_code=200

        return result

# 已學課程統計
@restapi.resource('/fulearn/V4/Department/CourseStatistic')
class FulearnV4DepartmentCourseStatistic(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default value of groups
        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'

        # store result
        result = []

        # 如果網址列有參數就取代
        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        sql = '''
        SELECT * FROM fulearn_4_view.Department_by_DcourseData as dbd
        LEFT JOIN fulearn_4.CourseNameList as fvc
        ON dbd.CourseId = fvc.CourseId
        WHERE User_Group1 = "{0}"
        AND User_Group2 = "{1}"
        AND User_Group3="{2}"
        '''.format(group1, group2, group3)

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
                        'courseName': row['CourseName'],
                        'courseId': row['CourseId'],
                        'depAvgScore': row['agvscoreall'] or -1,
                        'depPassRate': row['prate'] or -1,
                        'studentAvgScore':row['agvscore'] or -1,
                        'depRank': row['rank'] or -1,
                        'learnAvgTime': row['avgtime'] or -1,
                        'learnPeople': row['students'] or -1
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/CourseStatistic failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

# 已學課程 > 分段分數人數
@restapi.resource('/fulearn/V4/Department/Exam/CumulativePeople')
class FulearnV4DepartmentExamCumulativePeople(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # 這個ＡＰＩ必須要給所有五個參數才回傳值, 少一個都不行
        if ('group1' not in request.args or 'group2' not in request.args
        or 'group3' not in request.args or 'courseId' not in request.args):
            response = jsonify([{
                    "message":"You have missing parameters, please check your request",
                    "status": "fail"
            }])
            response.status_code = 400
            # return result with content-type/json
            return response

        # 四個參數都有
        group1 = request.args['group1']
        group2 = request.args['group2']
        group3 = request.args['group3']
        courseId = request.args['courseId']

        # 查詢結果預設值
        result = []
        for i in range(0,10):
            keys = ['0-10', '10-20', '20-30', '30-40', '40-50',
            '50-60', '60-70', '70-80', '80-90', '90-100']

            result.append({
                'group1': group1,
                'group2': group2,
                'group3': group3,
                'courseId': courseId,
                'scope': keys[i],
                'count': 0
            })

        sql='''SELECT * FROM fulearn_4_view.Department_by_scoreGroup
        WHERE User_Group1="{0}"
        AND User_Group2="{1}"
        AND User_Group3="{2}"
        AND CourseId="{3}"
        '''.format(group1, group2, group3, courseId)

        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            if(cursor.rowcount > 0):
                for row in rows:
                    # check if the course id is in list
                    next(ele for ele in result if ele["scope"]==row['grade'])['count']+=row['amount']


        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/ExamCumulativePeople failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code = 200

        return response

# 錯題分析
@restapi.resource('/fulearn/V4/Department/Exam/ExamErrors')
class FulearnV4DepartmentExamErrors(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'
        data_length = 10

         # updata info by user input
        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']


        sql = '''SELECT * FROM fulearn_4_view.Department_by_raceWrongRate as fdb
        LEFT JOIN fulearn_4.CourseNameList as fc
        ON fdb.CourseId = fc.CourseId
        WHERE User_Group1="{0}"
        AND User_Group2="{1}"
        AND User_Group3="{2}"
        LIMIT {3}
        '''.format(group1, group2, group3, data_length)

        result = []

        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            if(cursor.rowcount > 0):
                for row in rows:
                    result.append({
                        'courseId': row['CourseId'],
                        'courseName': row['CourseName'],
                        'questionId': row['QuestionId'],
                        'errorRate': row['wrongrate'],
                        'group1': group1,
                        'group2': group2,
                        'group3': group3
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/ExamCumulativePeople failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code = 200

        return response


@restapi.resource('/fulearn/V4/Department/List/Group1')
class FulearnV4DepartmentListGroup1(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        result = []

        sql = '''SELECT Group1 FROM fulearn_4.log_data_person WHERE Group1 IS NOT NULL GROUP BY Group1'''

        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            # have data
            if(cursor.rowcount > 0):
                for row in rows:
                    result.append({
                        'group1': row['Group1']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/ExamCumulativePeople failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code = 200

        return response

@restapi.resource('/fulearn/V4/Department/List/Group2')
class FulearnV4DepartmentListGroup2(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        result = []

        sql = '''SELECT Group2 FROM fulearn_4.log_data_person WHERE Group2 IS NOT NULL GROUP BY Group2'''

        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # have data
            if(cursor.rowcount > 0):
                for row in rows:
                    result.append({
                        'group2': row['Group2']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/ExamCumulativePeople failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code = 200

        return response

@restapi.resource('/fulearn/V4/Department/List/Group3')
class FulearnV4DepartmentListGroup3(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        result = []

        sql = '''SELECT Group3 FROM fulearn_4.log_data_person WHERE Group3 IS NOT NULL GROUP BY Group3'''

        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # have data
            if(cursor.rowcount > 0):
                for row in rows:
                    result.append({
                        'group3': row['Group3']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/ExamCumulativePeople failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code = 200

        return response

# for new fulearn API

@restapi.resource('/fulearn/V4/department/members')
class FulearnV4DepartmentMembers(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'

         # updata info by user input
        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']


        sql = '''SELECT * FROM fulearn_4_view.DepartmentMembers
        WHERE Group1="{0}"
        AND Group2="{1}"
        AND Group3="{2}"
        '''.format(group1, group2, group3)

        result = []

        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            if(cursor.rowcount > 0):
                for row in rows:
                    result.append({
                        'id': row['id']
                        ,'name': row['name']
                        ,'position': row['position']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/ExamCumulativePeople failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code = 200

        return response

@restapi.resource('/fulearn/V4/department/statistic2')
class FulearnV4DepartmentStatistic2(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'

         # updata info by user input
        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']


        sql = '''SELECT * FROM fulearn_4_view.DepartmentStatistic
        WHERE group1="{0}"
        AND group2="{1}"
        AND group3="{2}"
        '''.format(group1, group2, group3)

        result = []

        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            if(cursor.rowcount > 0):
                for row in rows:
                    result.append({
                        'sumMembers': row['sumMembers']
                        ,'sumCourses': row['sumCourses']
                        ,'sumCredits': str(row['sumCredits'])
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/ExamCumulativePeople failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code = 200

        return response


@restapi.resource('/fulearn/V4/department/monthLearnTime2')
class FulearnV4DepartmentMonthLearnTime2(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        # default values
        year = '2017'
        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'

        # store result
        result = []

        # update parameters
        if 'year' in request.args:
            year = request.args['year']

        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        sql=''' SELECT * FROM fulearn_4_view.DepartmentMonthLearnTime
        WHERE group1 = "{0}"
        AND group2 = "{1}"
        AND group3 = "{2}"
        AND year="{3}"
        '''.format(group1, group2, group3, year)

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
                        'month': r['month'],
                        'year':r['year'],
                        'learnTime': r['learnTime']
                    })
            else:
                pass
        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/LearnTime failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200

        return result


@restapi.resource('/fulearn/V4/department/courseLearnTime2')
class FulearnV4DepartmentCourseLearnTime2(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default values
        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'
        data_length = 10

        # store result
        result = []

        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        if 'dataLength' in request.args:
            data_length = request.args['dataLength']

        sql='''SELECT * FROM fulearn_4_view.DepartmentCourseLearnTime
        WHERE group1="{0}"
        AND group2="{1}"
        AND group3="{2}"
        ORDER BY learnTime DESC
        LIMIT {3}
        '''.format(group1, group2, group3, data_length)
        print(sql)

        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            # excute sql
            cursor.execute(sql)
            rows = cursor.fetchall()

            if cursor.rowcount>0:
                for r in rows:
                    result.append({
                        'courseName': r['courseName'],
                        'learnTime': r['learnTime']
                    })
            else:
                result = []

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/CourseLearnTime failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200

        return result


@restapi.resource('/fulearn/V4/department/abliltyRadar')
class FulearnV4DepartmentAbliltyRadar(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default values
        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'

        # store result
        result = []

        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        sql='''SELECT * FROM fulearn_4_view.DepartmentAbliltyRadar
        WHERE group1="{0}"
        AND group2="{1}"
        AND group3="{2}"
        '''.format(group1, group2, group3)
        print(sql)

        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            if cursor.rowcount>0:
                for r in rows:
                    result.append({
                        'courceTypeID': r['courceTypeID'],
                        'courceTypeName': r['courceTypeName'],
                        'percent': r['percent']
                    })
            else:
                result = []

        except Exception as inst:
            logging.getLogger('error_Logger').error('/fulearn/V4/Department/CourseLearnTime failed')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200

        return result


@restapi.resource('/fulearn/V4/department/learnedCourse')
class FulearnV4DepartmentLearnedCourse(Resource):
    @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default value of groups
        group1 = '總部周邊'
        group2 = 'IE總處'
        group3 = '資訊科技'

        # store result
        result = []

        # 如果網址列有參數就取代
        if 'group1' in request.args:
            group1 = request.args['group1']

        if 'group2' in request.args:
            group2 = request.args['group2']

        if 'group3' in request.args:
            group3 = request.args['group3']

        sql = '''SELECT * FROM fulearn_4_view.DepartmentLearnedCourse
        WHERE Group1 = "{0}"
        AND Group2 = "{1}"
        AND Group3="{2}"
        '''.format(group1, group2, group3)

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
                        'courseName': row['courseName'],
                        'courseID': row['courseID'],
                        'avgDepLearnTime': row['avgDepLearnTime'],
                        'depLearnPeople': row['depLearnPeople'],
                        'wholeLearnPeople':row['wholeLearnPeople'],
                        'depRank': row['depRank'],
                        'avgWholeScore': row['avgWholeScore'],
                        'avgDepScore': row['avgDepScore']
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
