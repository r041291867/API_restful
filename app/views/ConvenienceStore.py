#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint,current_app, request, make_response, jsonify
from flask_restful import Resource, Api

from . import views_blueprint
from app.extensions import mongo,mysql2,restapi,cache
from app.utils import cache_key
from flask import request
import textwrap
import gzip
import logging
import json
import decimal
from kafka import KafkaProducer
import os
from datetime import date,datetime as dt, timedelta
import base64
from pymongo import ASCENDING, DESCENDING
import random
#from manage import app

@restapi.resource('/ConvenienceStore')
class ConvenienceStore(Resource):
        def get(self):
                return {'hello': 'world'}

# New API for vuetest
@restapi.resource('/ConvenienceStore/todayinfo')
class Todayinfo(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                result = []
                if 'date' in request.args:
                    date = request.args['date']
                    sql = '''SELECT K.createtime, K.KLL, D.`weekday`, D.`amount`, D.`N_person`, D.`N_sales`, D.`ori_amount`, D.`vip_amount`, D.`vipN_person`, D.`price_avg`, D.`rate`, D.`price_piece`, D.`area_effect`, D.`price_vip`, D.`percent_vip`, D.`discount`, D.`amount_growth_rate`, D.`order_growth_rate`, D.`d2d_growth_rate`, D.`TEM`
                    FROM Convenience_Store.LH_quota_day AS D CROSS JOIN Convenience_Store.LH_KLL_day AS K
                    ON D.date = K.createtime AND K.createtime <= '{0}' LIMIT 1
                    '''.format(date)
                #sql = '''SELECT * FROM Weather.FutureWeather AS F INNER JOIN Convenience_Store.LH_quota_day AS L ON F.Date = L.date ORDER BY L.date DESC LIMIT 1'''
                else :
                    sql = '''SELECT K.createtime, K.KLL, D.`weekday`, D.`amount`, D.`N_person`, D.`N_sales`, D.`ori_amount`, D.`vip_amount`, D.`vipN_person`, D.`price_avg`, D.`rate`, D.`price_piece`, D.`area_effect`, D.`price_vip`, D.`percent_vip`, D.`discount`, D.`amount_growth_rate`, D.`order_growth_rate`, D.`d2d_growth_rate`, D.`TEM`
                    FROM Convenience_Store.LH_quota_day AS D CROSS JOIN Convenience_Store.LH_KLL_day AS K
                    ON D.date = K.createtime LIMIT 1
                    '''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                        result.append({
                                                'curdate': row['createtime'].strftime('%Y-%m-%d'),
                                                'KLL': row['KLL'],
                                                'weekday': row['weekday'],
                                                'amount': row['amount'],
                                                'N_person': row['N_person'],
                                                'N_sales': row['N_sales'],
                                                'ori_amount': row['ori_amount'],
                                                'vip_amount': row['vip_amount'],
                                                'vipN_person': row['vipN_person'],
                                                'price_avg': row['price_avg'],
                                                'rate': row['rate'],
                                                'area_effect': row['area_effect'],
                                                'price_piece': row['price_piece'],
                                                'price_vip': row['price_vip'],
                                                'percent_vip': row['percent_vip'],
                                                'discount': row['discount'],
                                                'amount_growth_rate': row['amount_growth_rate'],
                                                'order_growth_rate': row['order_growth_rate'],
                                                'TEM': row['TEM'],
                                                'd2d_growth_rate': row['d2d_growth_rate']
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore todayinfo Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/amountmetric')
class Amountmetric(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                result = []
                if 'date' in request.args:
                    date = request.args['date']
                    sql = '''select t.date,
                          c.type,
                          case c.type
                            when '休闲食品' then 休闲食品
                            when '其他' then 其他
                            when '家居文体' then 家居文体
                            when '日用洗化' then 日用洗化
                            when '日配' then 日配
                            when '生鲜' then 生鲜
                            when '粮油副食' then 粮油副食
                            when '酒饮冲调' then 酒饮冲调
                            when '餐饮原材料' then 餐饮原材料
                            when '餐饮原物料' then 餐饮原物料
                            when '香烟' then 香烟
                          end as amount
                        from (SELECT * FROM Convenience_Store.LH_type_amount_day WHERE date <= '{0}' ORDER BY date DESC LIMIT 1) AS t
                        cross join
                        (
                          select '休闲食品' as type
                          union all select '其他'
                          union all select '家居文体'
                          union all select '日用洗化'
                          union all select '日配'
                          union all select '生鲜'
                          union all select '粮油副食'
                          union all select '酒饮冲调'
                          union all select '餐饮原材料'
                          union all select '餐饮原物料'
                          union all select '香烟'
                        ) c ORDER BY amount DESC'''.format(date)
                else :
                    sql = '''select t.date,
                          c.type,
                          case c.type
                            when '休闲食品' then 休闲食品
                            when '其他' then 其他
                            when '家居文体' then 家居文体
                            when '日用洗化' then 日用洗化
                            when '日配' then 日配
                            when '生鲜' then 生鲜
                            when '粮油副食' then 粮油副食
                            when '酒饮冲调' then 酒饮冲调
                            when '餐饮原材料' then 餐饮原材料
                            when '餐饮原物料' then 餐饮原物料
                            when '香烟' then 香烟
                          end as amount
                        from (SELECT * FROM Convenience_Store.LH_type_amount_day ORDER BY date DESC LIMIT 1) AS t
                        cross join
                        (
                          select '休闲食品' as type
                          union all select '其他'
                          union all select '家居文体'
                          union all select '日用洗化'
                          union all select '日配'
                          union all select '生鲜'
                          union all select '粮油副食'
                          union all select '酒饮冲调'
                          union all select '餐饮原材料'
                          union all select '餐饮原物料'
                          union all select '香烟'
                        ) c ORDER BY amount DESC'''

                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                sum = 0
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                    sum += row['amount'];
                                for row in rows:
                                        result.append({
                                                'date': row['date'],
                                                'type': row['type'],
                                                'amount': row['amount'],
                                                'contribution': round((row['amount']/sum), 4)
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore amountmetric Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/orderhour24')
class Orderhour24(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                result = []
                #sql = '''SELECT * FROM Convenience_Store.LH_order_hour ORDER BY date DESC, hour ASC LIMIT 24'''
                sql = '''SELECT O.date, O.N_person, R.KLL, R.hour
                    FROM Convenience_Store.LH_order_hour AS O CROSS JOIN Convenience_Store.LH_KLL_hour AS R
                    ON O.date = R.date and O.hour = R.hour
                    ORDER BY date DESC, hour ASC LIMIT 24
                    '''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                        result.append({
                                                'date': row['date'],
                                                'N_person': row['N_person'],
                                                'hours': int(row['hour']),
                                                'pronum': row['KLL']
                                                #'pronum': (random.randint(0, 100))
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore orderhour24 Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/amountmetric_week')
class Amountmetricweek(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                result = []
                sql = '''SELECT t.week_no,
                        c.type,
                        case c.type
                          when '休闲食品' then 休闲食品
                          when '其他' then 其他
                          when '家居文体' then 家居文体
                          when '日用洗化' then 日用洗化
                          when '日配' then 日配
                          when '生鲜' then 生鲜
                          when '粮油副食' then 粮油副食
                          when '酒饮冲调' then 酒饮冲调
                          when '餐饮原材料' then 餐饮原材料
                          when '餐饮原物料' then 餐饮原物料
                          when '香烟' then 香烟
                        end as amount
                      from (SELECT * FROM Convenience_Store.LH_type_amount_week ORDER BY year DESC LIMIT 1) AS t
                      cross join
                      (
                          SELECT '休闲食品' as type
                          union all SELECT '其他'
                          union all SELECT '家居文体'
                          union all SELECT '日用洗化'
                          union all SELECT '日配'
                          union all SELECT '生鲜'
                          union all SELECT '粮油副食'
                          union all SELECT '酒饮冲调'
                          union all SELECT '餐饮原材料'
                          union all SELECT '餐饮原物料'
                          union all SELECT '香烟'
                      ) c
                    ORDER BY amount DESC'''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                sum = 0
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                    sum += row['amount'];
                                for row in rows:
                                        result.append({
                                                'week_no': row['week_no'],
                                                'type': row['type'],
                                                'amount': row['amount'],
                                                'contribution': round((row['amount']/sum), 4)
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore amountmetric_week Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/week_last7_amounttem')
class Weeklast7amounttem(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                result = []
                sql = '''SELECT CONVERT(`date`, date) as date, `weekday`, `TEM`, `amount` FROM Convenience_Store.LH_quota_day ORDER BY date DESC LIMIT 7'''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                        result.append({
                                                'date': row['date'].strftime("%Y-%m-%d"),
                                                'weekday': row['weekday'],
                                                'TEM': row['TEM'],
                                                'amount': row['amount']
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore week_last7_amounttem Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/week_sixindicators')
class Week_sixindicators(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                result = []
                thisweekpeople = [3225, 7141, 1325, 6256, 4141, 3325, 6256 ]
                lastweekpeople = [3125, 3141, 3225, 7256, 1141, 4325, 2256 ]
                thisweekdealrate = [35, 74, 25, 56, 71, 25, 56 ]
                lastweekdealrate = [25, 71, 32, 62, 41, 32, 66 ]
                thisweek = []
                lastweek = []
                thisweekdate = []
                lastweekdate = []
                thisweekweekday = []
                lastweekweekday = []
                thisweekrate = []
                lastweekrate = []
                thisweekN_person = []
                lastweekN_person = []
                thisweekprice_piece = []
                lastweekprice_piece = []
                thisweekprice_avg = []
                lastweekprice_avg = []
                sql = '''SELECT CONVERT(`date`, date) as date,`weekday`,`price_piece`,`rate`,`price_avg`,`N_person` FROM Convenience_Store.LH_quota_day ORDER BY date DESC LIMIT 14'''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                            for i in range(len(rows)-7):
                                thisweek.append(rows[i])
                            for i in range(len(rows)-7, 14):
                                lastweek.append(rows[i])
                            for tw in thisweek:
                                thisweekdate.append(tw['date'].strftime("%Y-%m-%d"))
                                thisweekweekday.append(tw['weekday'])
                                thisweekrate.append(tw['rate'])
                                thisweekN_person.append(tw['N_person'])
                                thisweekprice_piece.append(tw['price_piece'])
                                thisweekprice_avg.append(tw['price_avg'])
                            for lw in lastweek:
                                lastweekdate.append(lw['date'].strftime("%Y-%m-%d"))
                                lastweekweekday.append(lw['weekday'])
                                lastweekrate.append(lw['rate'])
                                lastweekN_person.append(lw['N_person'])
                                lastweekprice_piece.append(lw['price_piece'])
                                lastweekprice_avg.append(lw['price_avg'])
                            result.append({
                                    'thisweekrate': thisweekrate,
                                    'thisweekN_person': thisweekN_person,
                                    'thisweekprice_piece': thisweekprice_piece,
                                    'thisweekprice_avg': thisweekprice_avg,
                                    'thisweekdate': thisweekdate,
                                    'thisweekweekday': thisweekweekday,
                                    'thisweekpeople': thisweekpeople,
                                    'thisweekdealrate': thisweekdealrate,
                                    'lastweekdate': lastweekdate,
                                    'lastweekweekday': lastweekweekday,
                                    'lastweekrate': lastweekrate,
                                    'lastweekN_person': lastweekN_person,
                                    'lastweekprice_piece': lastweekprice_piece,
                                    'lastweekprice_avg': lastweekprice_avg,
                                    'lastweekpeople': lastweekpeople,
                                    'lastweekdealrate': lastweekdealrate
                            })
                        # print(thisweek)
                        # print(lastweek)

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore week_sixindicators Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/amountmetric_mon')
class Amountmetricmon(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                result = []
                sql = '''SELECT t.date,
                        c.type,
                        case c.type
                          when '休闲食品' then 休闲食品
                          when '其他' then 其他
                          when '家居文体' then 家居文体
                          when '日用洗化' then 日用洗化
                          when '日配' then 日配
                          when '生鲜' then 生鲜
                          when '粮油副食' then 粮油副食
                          when '酒饮冲调' then 酒饮冲调
                          when '餐饮原材料' then 餐饮原材料
                          when '餐饮原物料' then 餐饮原物料
                          when '香烟' then 香烟
                        end as amount
                      from (SELECT * FROM Convenience_Store.LH_type_amount_mon ORDER BY date DESC LIMIT 1) AS t
                      cross join
                      (
                          SELECT '休闲食品' as type
                          union all SELECT '其他'
                          union all SELECT '家居文体'
                          union all SELECT '日用洗化'
                          union all SELECT '日配'
                          union all SELECT '生鲜'
                          union all SELECT '粮油副食'
                          union all SELECT '酒饮冲调'
                          union all SELECT '餐饮原材料'
                          union all SELECT '餐饮原物料'
                          union all SELECT '香烟'
                      ) c
                    ORDER BY amount DESC'''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                sum = 0
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                    sum += row['amount'];
                                for row in rows:
                                        result.append({
                                                'date': row['date'],
                                                'type': row['type'],
                                                'amount': row['amount'],
                                                'contribution': round((row['amount']/sum), 4)
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore amountmetric_mon Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/mon_last_amounttem')
class Monlastamounttem(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                result = []
                month = 0
                year = 0
                sql = '''SELECT CONVERT(`date`, date) as date FROM Convenience_Store.LH_quota_day ORDER BY date DESC LIMIT 1'''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        year = rows[0]['date'].strftime('%Y')
                        month = rows[0]['date'].strftime('%m')
                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore mon_last_amounttem Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                query = year+"-"+month+"%"

                sql2 = '''SELECT CONVERT(`date`, date) as date, `TEM`, `amount`
                FROM Convenience_Store.LH_quota_day
                WHERE date LIKE '{0}' ORDER BY date ASC
                '''.format(query)
                print(sql2)
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql2)
                        rows_q = cursor.fetchall()
                        # data exist
                        if len(rows_q)>0:
                                for row in rows_q:
                                        result.append({
                                                'date': row['date'].strftime("%Y-%m-%d"),
                                                'TEM': row['TEM'],
                                                'amount': row['amount']
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore mon_last_amounttem Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/mon_sixindicators')
class Monsixindicators(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                def month_addzero(num):
                    numstr = ''
                    if num < 10:
                        numstr = "0"+str(num)
                    else:
                        numstr = str(num)
                    return numstr

                result = []
                month = 0
                year = 0
                day = 0
                sql0 = '''SELECT CONVERT(`date`, date) as date FROM Convenience_Store.LH_quota_day ORDER BY date DESC LIMIT 1'''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql0)
                        rows = cursor.fetchall()
                        year = rows[0]['date'].strftime('%Y')
                        month = rows[0]['date'].strftime('%m')
                        day = rows[0]['date'].strftime('%d')
                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore mon_last_amounttem Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                query = year+"-"+month+"%"
                lastmonend = year+"-"+str(month_addzero(int(month)-1))+"-"+day
                lastmonstart = year+"-"+str(month_addzero(int(month)-1))+"-01"
                # print(query)
                # print(lastmonend)
                # print(lastmonstart)
                #
                thisweekpeople = []
                lastweekpeople = []
                thisweekdealrate = []
                lastweekdealrate = []
                thisweek = []
                lastweek = []
                thisweekdate = []
                lastweekdate = []
                thisweekrate = []
                lastweekrate = []
                thisweekN_person = []
                lastweekN_person = []
                thisweekprice_piece = []
                lastweekprice_piece = []
                thisweekprice_avg = []
                lastweekprice_avg = []
                sql = '''SELECT CONVERT(`date`, date) as date,`price_piece`,`rate`,`price_avg`,`N_person` FROM Convenience_Store.LH_quota_day
                WHERE date LIKE '{0}' OR (date <= '{1}' AND date >= '{2}') ORDER BY date ASC
                '''.format(query, lastmonend, lastmonstart)
                # connector to mysql
                print(sql)
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                            # print(len(rows)/2)
                            for i in range(len(rows)-int(len(rows)/2)):
                                lastweek.append(rows[i])
                            for i in range(len(rows)-int(len(rows)/2), len(rows)):
                                thisweek.append(rows[i])
                            for tw in thisweek:
                                thisweekpeople.append(random.randint(0, 8000))
                                thisweekdealrate.append(round(random.random(), 4))
                                thisweekdate.append(tw['date'].strftime("%Y-%m-%d"))
                                thisweekrate.append(tw['rate'])

                                thisweekN_person.append(tw['N_person'])
                                thisweekprice_piece.append(tw['price_piece'])
                                thisweekprice_avg.append(tw['price_avg'])
                            for lw in lastweek:
                                lastweekpeople.append(random.randint(0, 8000))
                                lastweekdealrate.append(round(random.random(), 4))
                                lastweekdate.append(lw['date'].strftime("%Y-%m-%d"))
                                lastweekrate.append(lw['rate'])
                                lastweekN_person.append(lw['N_person'])
                                lastweekprice_piece.append(lw['price_piece'])
                                lastweekprice_avg.append(lw['price_avg'])
                            result.append({
                                    'thisweekrate': thisweekrate,
                                    'thisweekN_person': thisweekN_person,
                                    'thisweekprice_piece': thisweekprice_piece,
                                    'thisweekprice_avg': thisweekprice_avg,
                                    'thisweekdate': thisweekdate,
                                    'thisweekpeople': thisweekpeople,
                                    'thisweekdealrate': thisweekdealrate,
                                    'lastweekdate': lastweekdate,
                                    'lastweekrate': lastweekrate,
                                    'lastweekN_person': lastweekN_person,
                                    'lastweekprice_piece': lastweekprice_piece,
                                    'lastweekprice_avg': lastweekprice_avg,
                                    'lastweekpeople': lastweekpeople,
                                    'lastweekdealrate': lastweekdealrate
                            })
                        # print(lastweek)

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore mon_sixindicators Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/mon_sixindicators_yaliang')
class Monsixindicatorsyaliang(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                def month_addzero(num):
                    numstr = ''
                    if num < 10:
                        numstr = "0"+str(num)
                    else:
                        numstr = str(num)
                    return numstr

                result = []
                month = 0
                year = 0
                day = 0
                if 'date' in request.args:
                    date = request.args['date']
                    sql0 = '''SELECT CONVERT(`date`, date) as date FROM Convenience_Store.LH_quota_day 
                    WHERE date <= '{0}'
                    ORDER BY date DESC LIMIT 1'''.format(date)
                else :
                    sql0 = '''SELECT CONVERT(`date`, date) as date FROM Convenience_Store.LH_quota_day ORDER BY date DESC LIMIT 1'''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql0)
                        rows = cursor.fetchall()
                        year = rows[0]['date'].strftime('%Y')
                        month = rows[0]['date'].strftime('%m')
                        day = rows[0]['date'].strftime('%d')
                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore mon_last_amounttem Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                query = year+"-"+month+"%"
                lastmonend = year+"-"+str(month_addzero(int(month)-1))+"-"+str(month_addzero(int(day)+1))
                lastmonstart = year+"-"+str(month_addzero(int(month)-1))+"-01"
                # print(query)
                # print(lastmonend)
                # print(lastmonstart)
                #
                thisweekpeople = []
                thisweekdealrate = []
                thisweek = []
                thisweekdate =[]
                lastweek = []
                lastweekdate = []
                lastweekpeople = []
                lastweekdealrate = []
                # sql = '''SELECT D.date, Y.KLL, D.N_person
                # FROM Convenience_Store.yaliangyun_Get_Multi_Point_Combination AS Y LEFT JOIN Convenience_Store.LH_quota_day AS D ON CONVERT(Y.starttime, date) = D.date
                # WHERE starttime LIKE '{0}' OR (starttime <= '{1}' AND starttime >= '{2}') ORDER BY starttime ASC
                # '''.format(query, lastmonend, lastmonstart)
                if 'date' in request.args:
                    date = request.args['date']
                    sql = '''SELECT D.date, K.KLL, D.N_person
                    FROM Convenience_Store.LH_quota_day AS D JOIN Convenience_Store.LH_KLL_day AS K ON D.date =CONVERT(K.createtime, date)
                    WHERE (D.date LIKE '{0}' AND D.date <= '{1}') OR (createtime <= '{2}' AND createtime >= '{3}') ORDER BY createtime ASC
                    '''.format(query, date, lastmonend, lastmonstart)
                else :
                    sql = '''SELECT D.date, K.KLL, D.N_person
                    FROM Convenience_Store.LH_quota_day AS D JOIN Convenience_Store.LH_KLL_day AS K ON D.date =CONVERT(K.createtime, date)
                    WHERE D.date LIKE '{0}' OR (createtime <= '{1}' AND createtime >= '{2}') ORDER BY createtime ASC
                    '''.format(query, lastmonend, lastmonstart)
                # connector to mysql
                print(sql)
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                            # print(len(rows)/2)
                            # for i in range(len(rows)-int(len(rows)/2)):
                            #     lastweek.append(rows[i])
                            for i in range(0,len(rows)):
                                thisweek.append(rows[i])
                            # print(thisweek)
                            for tw in thisweek:
                                thisweekpeople.append(tw['KLL']);
                                thisweekdealrate.append(tw['N_person'])
                                thisweekdate.append((tw['date']).strftime("%Y-%m-%d"))
                            result.append({
                                    'thisweekdate': thisweekdate,
                                    'thisweekpeople': thisweekpeople,
                                    'thisweekdealrate': thisweekdealrate
                            })
                        # print(lastweek)

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore mon_sixindicators Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/week_sixindicators_yaliang')
class Weeksixindicatorsyaliang(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                def month_addzero(num):
                    numstr = ''
                    if num < 10:
                        numstr = "0"+str(num)
                    else:
                        numstr = str(num)
                    return numstr

                result = []
                month = 0
                year = 0
                day = 0
                sql0 = '''SELECT CONVERT(`date`, date) as date FROM Convenience_Store.LH_quota_day ORDER BY date DESC LIMIT 1'''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql0)
                        rows = cursor.fetchall()
                        year = rows[0]['date'].strftime('%Y')
                        month = rows[0]['date'].strftime('%m')
                        day = rows[0]['date'].strftime('%d')
                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore mon_last_amounttem Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                query = year+"-"+month+"-"+month_addzero(int(day)+1)
                lastmonend = year+"-"+str(month_addzero(int(month)-1))+"-"+day
                lastmonstart = year+"-"+str(month_addzero(int(month)-1))+"-01"
                # print(query)
                # print(lastmonend)
                # print(lastmonstart)
                #
                thisweekpeople = []
                thisweekdealrate = []
                thisweek = []
                lastweek = []
                thisweekdate =[]
                lastweekdate = []
                lastweekpeople = []
                lastweekdealrate = []
                sql = '''SELECT D.date, K.KLL, D.N_person
                FROM Convenience_Store.LH_KLL_day AS K LEFT JOIN Convenience_Store.LH_quota_day AS D ON CONVERT(K.createtime, date) = D.date
                WHERE starttime <= '{0}' AND date IS NOT NULL
                ORDER BY starttime DESC LIMIT 14
                '''.format(query)
                # connector to mysql
                print(sql)
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                            # print(len(rows)/2)
                            for i in range(len(rows)-7, len(rows)):
                                lastweek.append(rows[i])
                            for i in range(0,len(rows)-7):
                                thisweek.append(rows[i])
                            # print(thisweek)
                            for lw in lastweek:
                                lastweekpeople.append(lw['KLL'])
                                lastweekdealrate.append(lw['N_person'])
                                lastweekdate.append(lw['date'].strftime("%Y-%m-%d"))
                            for tw in thisweek:
                                thisweekpeople.append(tw['KLL']);
                                thisweekdealrate.append(tw['N_person'])
                                thisweekdate.append((tw['date']).strftime("%Y-%m-%d"))
                            result.append({
                                    'thisweekdate': thisweekdate,
                                    'thisweekpeople': thisweekpeople,
                                    'thisweekdealrate': thisweekdealrate,
                                    'lastweekpeople': lastweekpeople,
                                    'lastweekdealrate': lastweekdealrate,
                                    'lastweekdate': lastweekdate
                            })
                        elif len(rows) < 15:
                            print("small")

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore mon_sixindicators Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result


# Test
@restapi.resource('/ConvenienceStore/Test')
class ConvenienceStore_Test(Resource):
        def get(self, headers=None):
                result = {"sum_sell_money":0}
                try :

                        conn = mysql2.connect()
                        cursor = conn.cursor()
                        cursor.execute("select sum(sell_money) as sum_sell_money from Convenience_Store.12_selldata")
                        rows = cursor.fetchall()
                        result['sum_sell_money'] = rows[0]['sum_sell_money']
                        cursor.close()
                        conn.close()
                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore Query Err')
                        logging.getLogger('error_Logger').error(inst)
                        print(str(inst))
                resp = make_response(
                                json.dumps(result ,
                                ensure_ascii=False))
                resp.headers.extend(headers or {})
                return resp

@restapi.resource('/ConvenienceStore/LH_goods_top30')
class LH_goods_top30(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default value of groups
        # jobID = 'F0300120'

        # store result
        result = []

        # 如果網址列有參數就取代
        # if 'id' in request.args:
        #     jobID = request.args['id']

        sql = '''SELECT * FROM Convenience_Store.LH_goods_top30'''

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'goods_name': row['goods_name'],
                        'sell_money': row['sell_money'],
                        'main_type': row['main_type'],
                        'contribution': row['contribution']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_goods_top30 Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_quota_day')
class LH_quota_day(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        result = []

        sql = '''SELECT * FROM Convenience_Store.LH_quota_day'''

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'date': row['date'].strftime("%Y-%m-%d"),
                        'weekday': row['weekday'],
                        'amount': row['amount'],
                        'N_person': row['N_person'],
                        'vip_amount': row['vip_amount'],
                        'vipN_person': row['vipN_person'],
                        'price_avg': row['price_avg'],
                        'rate': row['rate'],
                        'price_vip': row['price_vip'],
                        'percent_vip': row['percent_vip'],
                        'discount': row['discount'],
                        'change_rate': row['change_rate'],
                        'd2d_growth_rate': row['d2d_growth_rate']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_quota_day Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_quota_day_LP')
class LH_quota_day_LP(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        result = []
        sql = '''SELECT * FROM Convenience_Store.LH_quota_day ORDER BY date DESC LIMIT 2'''
        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'date': row['date'].strftime("%Y-%m-%d"),
                        'weekday': row['weekday'],
                        'amount': row['amount'],
                        'N_person': row['N_person'],
                        'vip_amount': row['vip_amount'],
                        'vipN_person': row['vipN_person'],
                        'price_avg': row['price_avg'],
                        'rate': row['rate'],
                        'price_vip': row['price_vip'],
                        'percent_vip': row['percent_vip'],
                        'discount': row['discount'],
                        'change_rate': row['change_rate'],
                        'd2d_growth_rate': row['d2d_growth_rate']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_quota_day_interval Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_quota_month_LP')
class LH_quota_month_LP(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        result = []
        sql = '''SELECT * FROM Convenience_Store.LH_quota_month ORDER BY date DESC LIMIT 2'''
        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'date': row['date'],
                        'amount': row['amount'],
                        'N_person': row['N_person'],
                        'vip_amount': row['vip_amount'],
                        'vipN_person': row['vipN_person'],
                        'price_avg': row['price_avg'],
                        'rate': row['rate'],
                        'price_vip': row['price_vip'],
                        'percent_vip': row['percent_vip'],
                        'discount': row['discount'],
                        'change_rate': row['change_rate'],
                        'm2m_growth_rate': row['m2m_growth_rate']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_quota_day_interval Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_quota_weekno_LP')
class LH_quota_weekno_LP(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        result = []
        sql = '''SELECT * FROM Convenience_Store.LH_quota_weekno ORDER BY year DESC LIMIT 2'''

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'year': row['year'],
                        'week_no': row['week_no'],
                        'amount': row['amount'],
                        'N_person': row['N_person'],
                        'vip_amount': row['vip_amount'],
                        'vipN_person': row['vipN_person'],
                        'price_avg': row['price_avg'],
                        'rate': row['rate'],
                        'price_vip': row['price_vip'],
                        'percent_vip': row['percent_vip'],
                        'discount': row['discount'],
                        'change_rate': row['change_rate'],
                        'w2w_growth_rate': row['w2w_growth_rate']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_quota_weekno Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_order_hour')
class LH_order_hour(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        result = []

        sql = '''SELECT * FROM Convenience_Store.LH_order_hour ORDER BY date DESC LIMIT 24'''

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'date': row['date'],
                        'hour': row['hour'],
                        'N_person': row['N_person']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_quota_weekno Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_quota_day_interval')
class LH_quota_day_interval(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        startdate = '2018-04-20'
        enddate = '2018-05-01'
        # store result
        result = []

        # 如果網址列有參數就取代
        if 's' in request.args:
            startdate = request.args['s']
        if 'e' in request.args:
            enddate = request.args['e']

        sql = '''SELECT * FROM Convenience_Store.LH_quota_day
        WHERE date >= '{0}' AND date <= '{1}'
        '''.format(startdate+' 00:00:00', enddate+' 00:00:00')

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            amount = 0
            vip_amount =0
            N_person = 0
            vipN_person = 0
            ori_amount = 0
            N_sales = 0
            if len(rows)>0:
                for row in rows:
                    amount += row['amount']
                    # vip_amount += int(row['vip_amount'].replace(',',''))
                    N_person += row['N_person']
                    # vipN_person += row['vipN_person']
                    # ori_amount += int(row['ori_amount'].replace(',',''))
                    N_sales += row['N_sales']

            result.append({
                'amount': amount,
                # 'vip_amount': vip_amount,
                'N_person': N_person,
                'N_sales': N_sales,
                'price_avg': round(amount/N_person,2),
                # 'vipN_person': vipN_person,
                # 'price_vip': round(vip_amount/vipN_person, 2),
                # 'percent_vip': round((vip_amount/amount)*100,2),
                # 'discount': round((amount/ori_amount)*100,2),
                'rate': round(N_sales/N_person, 2),
                'price_piece': round(amount/N_sales, 2),
                'area_effect': round(amount/200, 2)

            })
        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_quota_day_interval Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_quota_day_interval_detail')
class LH_quota_day_interval_detail(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        startdate = '2018-04-20'
        enddate = '2018-05-01'
        # store result
        result = []

        # 如果網址列有參數就取代
        if 's' in request.args:
            startdate = request.args['s']
        if 'e' in request.args:
            enddate = request.args['e']

        sql = '''SELECT * FROM Convenience_Store.LH_quota_day
        WHERE date >= '{0}' AND date <= '{1}'
        '''.format(startdate+' 00:00:00', enddate+' 00:00:00')

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'amount': row['amount'],
                        'TEM': row['TEM'],
                        'date': row['date'].strftime("%Y-%m-%d")
                    })


        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_quota_day_interval_detail Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_GDZ_temp')
class LH_GDZ_temp(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        startdate = '2018-04-20'
        enddate = '2018-05-01'
        # store result
        result = []

        # 如果網址列有參數就取代
        if 's' in request.args:
            startdate = request.args['s']
        if 'e' in request.args:
            enddate = request.args['e']

        sql = '''SELECT * FROM Convenience_Store.LH_GDZ_temp
        WHERE date >= '{0}' AND date <= '{1}'
        '''.format(startdate, enddate)

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'date': row['date'],
                        'amount': row['amount'],
                        'TEM': row['TEM']
                    })
        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_GDZ_temp Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/OperationKPI')
class OperationKPI(Resource):
    @cache.cached(timeout=6000, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default value of groups
        startdate = '2018-04-20 00:00:00'
        enddate = '2018-05-01 00:00:00'
        # store result
        result = []

        # 如果網址列有參數就取代
        if 's' in request.args:
            startdate = request.args['s']
        if 'e' in request.args:
            enddate = request.args['e']

        sql = '''SELECT * FROM Convenience_Store.LH_quota_day
        WHERE date >= '{0}' AND date <= '{1}'
        '''.format(startdate+' 00:00:00', enddate+' 00:00:00')

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'date': row['date'].strftime("%Y-%m-%d"),
                        'weekday': row['weekday'],
                        'amount': row['amount'],
                        'N_person': row['N_person'],
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore OperationKPI Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_quota_year')
class LH_quota_year(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        result = []

        sql = '''SELECT * FROM Convenience_Store.LH_quota_year'''

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'date': row['date'],
                        'amount': row['amount'],
                        'N_person': row['N_person'],
                        'vip_amount': row['vip_amount'],
                        'vipN_person': row['vipN_person'],
                        'price_avg': row['price_avg'],
                        'rate': row['rate'],
                        'price_vip': row['price_vip'],
                        'percent_vip': row['percent_vip'],
                        'discount': row['discount'],
                        'change_rate': row['change_rate']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_quota_year Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_quota_weekno')
class LH_quota_weekno(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        def month_addzero(num):
            numstr = ''
            if num < 10:
                numstr = "0"+str(num)
            else:
                numstr = str(num)
            return numstr

        result = []
        month = 0
        year = 0
        day = 0
        sql0 = '''SELECT CONVERT(`date`, date) as date FROM Convenience_Store.LH_quota_day ORDER BY date DESC LIMIT 1'''
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()
        try:
                cursor.execute(sql0)
                rows = cursor.fetchall()
                year = rows[0]['date'].strftime('%Y')
                month = rows[0]['date'].strftime('%m')
                day = rows[0]['date'].strftime('%d')
        except Exception as inst:
                logging.getLogger('error_Logger').error('ConvenienceStore mon_last_amounttem Query Err')
                logging.getLogger('error_Logger').error(inst)

        finally:
                cursor.close()
                conn.close()

        query = year+"-"+month+"-"+month_addzero(int(day)+1)
        result = []

        #sql = '''SELECT * FROM Convenience_Store.LH_quota_weekno ORDER BY year DESC LIMIT 1'''
        sql = '''
            SELECT R.KLL, R.gettype, R.curdate,W.week_no, W.year, W.amount, W.N_person, W.N_sales, W.ori_amount, W.vip_amount, W.vipN_person, W.price_avg, W.rate, W.price_piece, W.area_effect, W.price_vip, W.percent_vip, W.discount, W.amount_growth_rate, W.order_growth_rate, W.w2w_growth_rate
            FROM Convenience_Store.yaliangyun_Get_Passenger_Preview_Part1 AS R JOIN Convenience_Store.LH_quota_weekno AS W
            WHERE curdate='{0}'
            AND gettype=2
            ORDER BY W.year DESC, R.curdate DESC LIMIT 1
        '''.format(query)
        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'KLL': row['KLL'],
                        'curdate': row['curdate'].strftime("%Y-%m-%d"),
                        'gettype': row['gettype'],
                        'year': row['year'],
                        'week_no': row['week_no'],
                        'amount': row['amount'],
                        'N_person': row['N_person'],
                        'vip_amount': row['vip_amount'],
                        'vipN_person': row['vipN_person'],
                        'price_avg': row['price_avg'],
                        'rate': row['rate'],
                        'N_sales': row['N_sales'],
                        'ori_amount': row['ori_amount'],
                        'price_piece': row['price_piece'],
                        'area_effect': row['area_effect'],
                        'amount_growth_rate': row['amount_growth_rate'],
                        'price_vip': row['price_vip'],
                        'percent_vip': row['percent_vip'],
                        'discount': row['discount'],
                        'order_growth_rate': row['order_growth_rate'],
                        'w2w_growth_rate': row['w2w_growth_rate']
                    })
        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_quota_weekno Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_quota_last_7_day')
class LH_quota_last_7_day(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        def month_addzero(num):
            numstr = ''
            if num < 10:
                numstr = "0"+str(num)
            else:
                numstr = str(num)
            return numstr

        result = []
        month = 0
        year = 0
        day = 0
        sql0 = '''SELECT CONVERT(`date`, date) as date FROM Convenience_Store.LH_quota_day ORDER BY date DESC LIMIT 1'''
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()
        try:
                cursor.execute(sql0)
                rows = cursor.fetchall()
                year = rows[0]['date'].strftime('%Y')
                month = rows[0]['date'].strftime('%m')
                day = rows[0]['date'].strftime('%d')
        except Exception as inst:
                logging.getLogger('error_Logger').error('ConvenienceStore mon_last_amounttem Query Err')
                logging.getLogger('error_Logger').error(inst)

        finally:
                cursor.close()
                conn.close()

        query = year+"-"+month+"-"+month_addzero(int(day)+1)
        result = []

        #sql = '''SELECT * FROM Convenience_Store.LH_quota_weekno ORDER BY year DESC LIMIT 1'''
        sql = '''
            SELECT SUM(S.KLL), SUM(S.amount), SUM(S.N_person), SUM(S.N_sales), SUM(S.N_sales)/SUM(S.N_person) AS rate, SUM(S.amount)/SUM(S.N_sales) AS price_piece, SUM(S.amount)/200 AS area_effect, SUM(S.amount)/SUM(S.N_person) AS price_avg
            FROM (SELECT K.KLL, D.amount, D.N_person, D.N_sales, D.ori_amount, D.vip_amount, D.vipN_person, D.price_avg, D.rate, D.price_piece, D.area_effect, D.price_vip, D.percent_vip, D.discount, D.amount_growth_rate, D.order_growth_rate
            FROM Convenience_Store.LH_KLL_day AS K JOIN Convenience_Store.LH_quota_day AS D ON K.createtime =CONVERT(D.date, date) 
            WHERE date<='{0}' AND date IS NOT NULL
            ORDER BY date DESC LIMIT 7) AS S
        '''.format(query)
        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'KLL': int(row['SUM(S.KLL)']),
                        'amount': int(row['SUM(S.amount)']),
                        'N_person': int(row['SUM(S.N_person)']),
                        'price_avg': round(float(row['price_avg']), 2),
                        'rate': round(float(row['rate']), 2),
                        'N_sales': int(row['SUM(S.N_sales)']),
                        'price_piece': round(float(row['price_piece']), 2),
                        'area_effect': round(float(row['area_effect']), 2)
                    })
        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_quota_last_7_day Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_quota_month')
class LH_quota_month(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        result = []
        month = '2018-05'
        if 'month' in request.args:
            month = request.args['month']

        #sql = '''SELECT * FROM Convenience_Store.LH_quota_month ORDER BY date DESC LIMIT 1'''
        sql = '''
            SELECT K.KLL, K.createtime, W.date, W.`amount`, W.`N_person`, W.`N_sales`, W.`ori_amount`, W.`vip_amount`, W.`vipN_person`, W.`price_avg`, W.`rate`, W.`price_piece`, W.`area_effect`, W.`price_vip`, W.`percent_vip`, W.`discount`, W.`amount_growth_rate`, W.`order_growth_rate`, W.`m2m_growth_rate`
            FROM Convenience_Store.LH_quota_month AS W INNER JOIN Convenience_Store.LH_KLL_month AS K
            WHERE K.createtime IN (W.date)
            ORDER BY K.createtime DESC, W.date DESC LIMIT 1
        '''
        # WHERE date <= '{0}'
        # '''.format(month)

        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'KLL': row['KLL'],
                        'curdate': row['createtime'],
                        'amount': row['amount'],
                        'N_person': row['N_person'],
                        'N_sales': row['N_sales'],
                        'ori_amount': row['ori_amount'],
                        'vip_amount': row['vip_amount'],
                        'vipN_person': row['vipN_person'],
                        'price_avg': row['price_avg'],
                        'rate': row['rate'],
                        'price_piece': row['price_piece'],
                        'area_effect': row['area_effect'],
                        'price_vip': row['price_vip'],
                        'percent_vip': row['percent_vip'],
                        'discount': row['discount'],
                        'amount_growth_rate': row['amount_growth_rate'],
                        'order_growth_rate': row['order_growth_rate'],
                        'm2m_growth_rate': row['m2m_growth_rate']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_quota_month Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/VIP_Describe')
class VIP_Describe(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):

        result = []
        CL = []
        EDU = []
        GEN = []
        MA = []
        SX = []

        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
        #     cursor.execute('''SELECT * FROM Convenience_Store.LH_vip_class''')
        #     rows = cursor.fetchall()
        #
        #     # data exist
        #     if len(rows)>0:
        #         for row in rows:
        #             CL.append({
        #                 'class': row['class'],
        #                 'N': row['N'],
        #                 'fraction': row['fraction']
        #             })
        #
        #     cursor.execute('''SELECT * FROM Convenience_Store.LH_vip_education''')
        #     rows_edu = cursor.fetchall()
        #
        #     # data exist
        #     if len(rows)>0:
        #         for row in rows_edu:
        #             EDU.append({
        #                 'education': row['education'],
        #                 'N': row['N'],
        #                 'fraction': row['fraction']
        #             })

            cursor.execute('''SELECT * FROM Convenience_Store.LH_vip_generation ORDER BY `Generation` ASC''')
            rows_gen = cursor.fetchall()

            # data exist
            if len(rows_gen)>0:
                for row in rows_gen:
                    GEN.append({
                        'Generation': row['Generation'],
                        'N': row['N'],
                        'fraction': row['fraction']
                    })

            cursor.execute('''SELECT * FROM Convenience_Store.LH_vip_marry''')
            rows_ma = cursor.fetchall()

            # data exist
            if len(rows_ma)>0:
                for row in rows_ma:
                    MA.append({
                        'Married': row['Married'],
                        'N': row['N'],
                        'fraction': row['fraction']
                    })
            cursor.execute('''SELECT * FROM Convenience_Store.LH_vip_sex''')
            rows_sx = cursor.fetchall()

            # data exist
            if len(rows_sx)>0:
                for row in rows_sx:
                    SX.append({
                        'Sex': row['Sex'],
                        'N': row['N'],
                        'fraction': row['fraction']
                    })

            # result.append(CL)
            # result.append(EDU)
            result.append({
                "GEN": GEN,
                "MA": MA,
                "SX": SX
            })
        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore VIP_Describe Query Err VIP')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_type_amount_day')
class LH_type_amount_day(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        # default value of groups
        # jobID = 'F0300120'

        # def month_addzero(num):
        #     numstr = ''
        #     if num < 10:
        #         numstr = "0"+str(num)
        #     else:
        #         numstr = str(num)
        #     return numstr
        #
        #
        # y = dt.now().strftime("%Y")
        # m = dt.now().strftime("%m")
        # d = dt.now().strftime("%d")
        # less = int(float(m))
        # lessd = int(float(d))
        # filter = y+ "-" +month_addzero(less)+"-"+month_addzero(lessd)
        # if 'date' in request.args:
        #     filter = request.args['date']
        result = []

        # sql = '''SELECT * FROM Convenience_Store.LH_type_amount_day
        #     WHERE `date` = "{0}"'''.format(filter)
        sql = '''SELECT * FROM Convenience_Store.LH_type_amount_day ORDER BY date DESC LIMIT 1'''
        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'date': row['date'],
                        'type1': row['休闲食品'],
                        'type2': row['其他'],
                        'type3': row['家居文体'],
                        'type4': row['日用洗化'],
                        'type5': row['日配'],
                        'type6': row['生鲜'],
                        'type7': row['粮油副食'],
                        'type8': row['酒饮冲调'],
                        'type9': row['餐饮原材料'],
                        'type10': row['餐饮原物料'],
                        'type11': row['香烟']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_type_amount_day Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_type_amount_mon')
class LH_type_amount_mon(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        result = []

        sql = '''SELECT * FROM Convenience_Store.LH_type_amount_mon ORDER BY date DESC LIMIT 1'''
        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'date': row['date'],
                        'type1': row['休闲食品'],
                        'type2': row['其他'],
                        'type3': row['家居文体'],
                        'type4': row['日用洗化'],
                        'type5': row['日配'],
                        'type6': row['生鲜'],
                        'type7': row['粮油副食'],
                        'type8': row['酒饮冲调'],
                        'type9': row['餐饮原材料'],
                        'type10': row['餐饮原物料'],
                        'type11': row['香烟']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_type_amount_mon Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_type_amount_week')
class LH_type_amount_week(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        result = []

        sql = '''SELECT * FROM Convenience_Store.LH_type_amount_week ORDER BY year DESC LIMIT 1'''
        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'year': row['year'],
                        'week_no': row['week_no'],
                        'type1': row['休闲食品'],
                        'type2': row['其他'],
                        'type3': row['家居文体'],
                        'type4': row['日用洗化'],
                        'type5': row['日配'],
                        'type6': row['生鲜'],
                        'type7': row['粮油副食'],
                        'type8': row['酒饮冲调'],
                        'type9': row['餐饮原材料'],
                        'type10': row['餐饮原物料'],
                        'type11': row['香烟']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_type_amount_week Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_type_contribution_day')
class LH_type_contribution_day(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        result = []

        sql = '''SELECT * FROM Convenience_Store.LH_type_contribution_day ORDER BY date DESC LIMIT 1'''
        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'date': row['date'],
                        'type1': row['休闲食品'],
                        'type2': row['其他'],
                        'type3': row['家居文体'],
                        'type4': row['日用洗化'],
                        'type5': row['日配'],
                        'type6': row['生鲜'],
                        'type7': row['粮油副食'],
                        'type8': row['酒饮冲调'],
                        'type9': row['餐饮原材料'],
                        'type10': row['餐饮原物料'],
                        'type11': row['香烟']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_type_contribution_day Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_type_contribution_mon')
class LH_type_contribution_mon(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        result = []

        sql = '''SELECT * FROM Convenience_Store.LH_type_contribution_mon ORDER BY date DESC LIMIT 1'''
        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'date': row['date'],
                        'type1': row['休闲食品'],
                        'type2': row['其他'],
                        'type3': row['家居文体'],
                        'type4': row['日用洗化'],
                        'type5': row['日配'],
                        'type6': row['生鲜'],
                        'type7': row['粮油副食'],
                        'type8': row['酒饮冲调'],
                        'type9': row['餐饮原材料'],
                        'type10': row['餐饮原物料'],
                        'type11': row['香烟']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_type_contribution_mon Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

@restapi.resource('/ConvenienceStore/LH_type_contribution_week')
class LH_type_contribution_week(Resource):
    # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
    def get(self, headers=None):
        result = []

        sql = '''SELECT * FROM Convenience_Store.LH_type_contribution_week ORDER BY year DESC LIMIT 1'''
        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'year': row['year'],
                        'week_no': row['week_no'],
                        'type1': row['休闲食品'],
                        'type2': row['其他'],
                        'type3': row['家居文体'],
                        'type4': row['日用洗化'],
                        'type5': row['日配'],
                        'type6': row['生鲜'],
                        'type7': row['粮油副食'],
                        'type8': row['酒饮冲调'],
                        'type9': row['餐饮原材料'],
                        'type10': row['餐饮原物料'],
                        'type11': row['香烟']
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore LH_type_contribution_week Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result


# SELECT * FROM `WeatherInfo3` WHERE `Province`='广东' AND `Area` = '深圳' AND Time <= '2018-05-01 23:59:59' AND Time >= '2018-05-01 00:00:00' ORDER BY `Time` DESC
# weather
# SELECT DATE_FORMAT(`date`,'%Y-%m-%d') AS `date` FROM Convenience_Store.LH_quota_day ORDER BY `date` DESC LIMIT 1
# SELECT * FROM `FutureWeather` WHERE `Area`='广东宝安' AND `date` IN (SELECT DATE_FORMAT(`date`,'%Y-%m-%d') AS `date` FROM Convenience_Store.LH_quota_day ORDER BY `date` DESC)
@restapi.resource('/ConvenienceStore/weatherToday')
class weatherToday(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                result = []
                query_time='2018-05-01'
                # 如果網址列有參數就取代
                if 'date' in request.args:
                    query_time = request.args['date']
                area = '''广东宝安'''
                sql = '''SELECT * FROM Weather.FutureWeather
                WHERE Area='{0}' AND `date`<='{1}'
                ORDER BY `date` DESC LIMIT 1
                '''.format(area, query_time)
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:

                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                        result.append({
                                                'Date': row['Date'].strftime('%Y-%m-%d'),
                                                'Area': row['Area'],
                                                'Weather': row['Weather'],
                                                'Low_Temp': row['Low_Temp'],
                                                'High_Temp': row['High_Temp'],
                                                'Wind': row['Wind'],
                                                'AQI': row['AQI']
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore weatherToday Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/weather')
class Weather(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):
                # default value of groups
                startdate = '2018-04-20'
                enddate = '2018-05-01'
                # store result
                result = []

                # 如果網址列有參數就取代
                if 's' in request.args:
                    startdate = request.args['s']
                if 'e' in request.args:
                    enddate = request.args['e']

                # 如果網址列有參數就取代
                sql = '''SELECT * FROM Weather.FutureWeather
                WHERE Date >= "{0}" AND Date <= "{1}"
                '''.format(startdate, enddate)
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:

                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                        result.append({
                                                'Date': row['Date'].strftime('%Y-%m-%d'),
                                                'Area': row['Area'],
                                                'Weather': row['Weather'],
                                                'Low_Temp': row['Low_Temp'],
                                                'High_Temp': row['High_Temp'],
                                                'Wind': row['Wind'],
                                                'AQI': row['AQI']
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore weather Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

# mongo
@restapi.resource('/ConvenienceStore/image_test_getLastTime')
class image_test_getLastTime(Resource):
	def get(self):

    		ImageBs = None
    		result = []

    		try :
    			count = list(mongo.cx['test'].JPHdataset.find().sort([('Time', DESCENDING)]).limit(1))[0]

    			print("image_test_getLastTime "+count['Time'])
    			# re = list(mongo.cx['test'].image_test.find({'Time':{'$gt': query_time}}).limit(1))[0]
    			# re =  mongo.cx['test'].image_test.find_one({},{'Time': -1}).get('Time',None)
    			# ImageBase64 = mongo.cx['test'].image_test.find_one({},{'image_base64':1}).get('image_base64',None)
    			result.append({'lasttime':count['Time']})
    		except Exception as inst:
    			print('ConvenienceStore image_test_getLastTime Query Err')
    			print(inst)
    		return result

@restapi.resource('/ConvenienceStore/image_test')
class image_test(Resource):
	def get(self):
    		# starttime = dt.now()
    		# print("start ")
    		# print(starttime)
    		ImageBs = None
    		Time = ''
    		re = []
    		query_time='2018-05-04 05:45:14'
            # 如果網址列有參數就取代
    		if 'time' in request.args:
    			query_time = request.args['time']
    		try :
    			# row = mongo.cx['test'].JPHdataset.count()
    			re = mongo.cx['test'].JPHdataset.find_one({'Time': {'$gt': query_time}})
    			# re = list(mongo.cx['test'].image_test.find({'Time':{'$gt': query_time}}).limit(1))[0]
    			# ImageBase64 = mongo.cx['test'].image_test.find_one({},{'image_base64':1}).get('image_base64',None)
    			# print(re['Time'])
    			Time = re['Image_base64']
    			# endtime = dt.now()
    			# print("end ")
    			# print(endtime)
    			# ImageBs = base64.b64decode(Time)
    		except IndexError as inst:
    			pass
    		except Exception as inst:
    			print('ConvenienceStore image_test Query Err')
    			print(inst)
    #			logging.getLogger('error_Logger').error('FulearnV4 Query UserKind Err')
    #			logging.getLogger('error_Logger').error(inst)
    		if Time:
    			resp = make_response(Time,200)
    			resp.headers['Content-Type'] = 'image'
    #			resp.headers.extend(headers or {})
    			return resp
    		return None

@restapi.resource('/ConvenienceStore/SpaceHeat')
class SpaceHeat(Resource):
	def get(self):
    		Time = ''
    		re = []
    		curdate='2018-06-18'
            # 如果網址列有參數就取代
    		if 'curdate' in request.args:
    			curdate = request.args['curdate']
    		try :
    			re = mongo.cx['Convenience_Store'].SpaceHeat.find_one({'curdate': curdate})
    			print(re['curdate'])
    			Time = re['Image_base64']

    		except Exception as inst:
    			print('ConvenienceStore SpaceHeat Query Err')
    			print(inst)
    		if Time:
    			resp = make_response(Time,200)
    			resp.headers['Content-Type'] = 'image'
    #			resp.headers.extend(headers or {})
    			return resp
    		return None

@restapi.resource('/ConvenienceStore/FaceRecord')
class FaceRecord(Resource):
    def get(self, headers=None):
        result = []

        sql = '''SELECT FaceFrame, Sex, Age FROM Convenience_Store.yaliangyun_ResolutionRecord_Newest ORDER BY Createtime DESC LIMIT 10'''
        print(sql)
        # connector to mysql
        conn = mysql2.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()

            # data exist
            if len(rows)>0:
                for row in rows:
                    result.append({
                        'FaceFrame': row['FaceFrame'],
                        'Sex': row['Sex'],
                        'Age': row['Age'],
                    })

        except Exception as inst:
            logging.getLogger('error_Logger').error('ConvenienceStore FaceRecord Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result

#富連網IT所需對接API
@restapi.resource('/ConvenienceStore/KLL24h')
class KLL24h(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                result = []
                #sql = '''SELECT * FROM Convenience_Store.LH_order_hour ORDER BY date DESC, hour ASC LIMIT 24'''
                sql = '''SELECT O.date, O.N_person, R.KLL, R.hour
                    FROM Convenience_Store.LH_order_hour AS O CROSS JOIN Convenience_Store.LH_KLL_hour AS R
                    ON O.date = R.date and O.hour = R.hour
                    ORDER BY date DESC, hour ASC LIMIT 24
                    '''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                        result.append({
                                                'date': row['date'],
                                                'hours': int(row['hour']),
                                                'pronum': row['KLL']
                                                #'pronum': (random.randint(0, 100))
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore KLL24h Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

#富連網IT所需對接API
@restapi.resource('/ConvenienceStore/CustomersDailyReport_D13')
class CustomersDailyReport_D13(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                def month_addzero(num):
                    numstr = ''
                    if num < 10:
                        numstr = "0"+str(num)
                    else:
                        numstr = str(num)
                    return numstr

                result = []
                month = 0
                year = 0
                day = 0
                sql0 = '''SELECT D.BeginDatetime FROM Convenience_Store_D13.CustomersDailyReport AS D ORDER BY D.BeginDatetime DESC LIMIT 1'''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql0)
                        rows = cursor.fetchall()
                        year = rows[0]['BeginDatetime'].strftime('%Y')
                        month = rows[0]['BeginDatetime'].strftime('%m')
                        day = rows[0]['BeginDatetime'].strftime('%d')
                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore CustomersDailyReport Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                result = []
                lastdayend = []
                lastdaystart = []
                #query = year+"-"+month+"%"
                lastdayend = str(year)+"-"+str(month_addzero(int(month)))+"-"+str(day)+" 23:59:59"
                lastdaystart = str(year)+"-"+str(month_addzero(int(month)))+"-"+str(day)+" 00:00:00"
                #print (lastdaystart)
                #print (lastdayend)
                sql = '''SELECT D.AreaCode, SUM(D.femalesage1) AS f1, SUM(D.femalesage2) AS f2, SUM(D.femalesage3) AS f3, SUM(D.femalesage4) AS f4, SUM(D.femalesage5) AS f5,
                SUM(D.malesage1) AS m1, SUM(D.malesage2) AS m2, SUM(D.malesage3) AS m3, SUM(D.malesage4) AS m4, SUM(D.malesage5) AS m5 
                FROM Convenience_Store_D13.CustomersDailyReport AS D
                WHERE (EndDatetime <= '{0}' AND BeginDatetime >= '{1}')
                GROUP BY D.AreaCode
                '''.format(lastdayend, lastdaystart)
    
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                        result.append({
                                                'BeginDatetime': lastdaystart,
                                                'EndDatetime': lastdayend,
                                                'AreaCode': row['AreaCode'],
                                                'KLL': int(row['f1'])+int(row['f2'])+int(row['f3'])+int(row['f4'])+int(row['f5'])+int(row['m1'])+int(row['m2'])+int(row['m3'])+int(row['m4'])+int(row['m5']),
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore CustomersDailyReport Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/CustomersDailyReport_feihu')
class CustomersDailyReport_feihu(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                def month_addzero(num):
                    numstr = ''
                    if num < 10:
                        numstr = "0"+str(num)
                    else:
                        numstr = str(num)
                    return numstr

                result = []
                month = 0
                year = 0
                day = 0
                sql0 = '''SELECT D.BeginDatetime FROM Convenience_Store_feihu.CustomersDailyReport AS D ORDER BY D.BeginDatetime DESC LIMIT 1'''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql0)
                        rows = cursor.fetchall()
                        year = rows[0]['BeginDatetime'].strftime('%Y')
                        month = rows[0]['BeginDatetime'].strftime('%m')
                        day = rows[0]['BeginDatetime'].strftime('%d')
                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore CustomersDailyReport Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                result = []
                lastdayend = []
                lastdaystart = []
                #query = year+"-"+month+"%"
                lastdayend = str(year)+"-"+str(month_addzero(int(month)))+"-"+str(day)+" 23:59:59"
                lastdaystart = str(year)+"-"+str(month_addzero(int(month)))+"-"+str(day)+" 00:00:00"
                #print (lastdaystart)
                #print (lastdayend)
                sql = '''SELECT D.AreaCode, SUM(D.femalesage1) AS f1, SUM(D.femalesage2) AS f2, SUM(D.femalesage3) AS f3, SUM(D.femalesage4) AS f4, SUM(D.femalesage5) AS f5,
                SUM(D.malesage1) AS m1, SUM(D.malesage2) AS m2, SUM(D.malesage3) AS m3, SUM(D.malesage4) AS m4, SUM(D.malesage5) AS m5 
                FROM Convenience_Store_feihu.CustomersDailyReport AS D
                WHERE (EndDatetime <= '{0}' AND BeginDatetime >= '{1}')
                GROUP BY D.AreaCode
                '''.format(lastdayend, lastdaystart)
    
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                        result.append({
                                                'BeginDatetime': lastdaystart,
                                                'EndDatetime': lastdayend,
                                                'AreaCode': row['AreaCode'],
                                                'KLL': int(row['f1'])+int(row['f2'])+int(row['f3'])+int(row['f4'])+int(row['f5'])+int(row['m1'])+int(row['m2'])+int(row['m3'])+int(row['m4'])+int(row['m5']),
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore CustomersDailyReport Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result

@restapi.resource('/ConvenienceStore/orderhour24_test')
class Orderhour24_test(Resource):
        # @cache.cached(timeout=600, key_prefix=cache_key, unless=None)
        def get(self, headers=None):

                date = ''
                # store result
                result = []

                # 如果網址列有參數就取代
                if 'date' in request.args:
                    date = request.args['date']
                    sql = '''SELECT O.date, O.N_person, R.KLL, R.hour
                        FROM Convenience_Store.LH_order_hour AS O CROSS JOIN Convenience_Store.LH_KLL_hour AS R
                        ON R.date <= '{0}' and O.date = R.date and O.hour = R.hour
                        ORDER BY date DESC, hour ASC LIMIT 24
                    '''.format(date)

                else :
                    #sql = '''SELECT * FROM Convenience_Store.LH_order_hour ORDER BY date DESC, hour ASC LIMIT 24'''
                    sql = '''SELECT O.date, O.N_person, R.KLL, R.hour
                        FROM Convenience_Store.LH_order_hour AS O CROSS JOIN Convenience_Store.LH_KLL_hour AS R
                        ON O.date = R.date and O.hour = R.hour
                        ORDER BY date DESC, hour ASC LIMIT 24
                    '''
                # connector to mysql
                conn = mysql2.connect()
                cursor = conn.cursor()
                try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        # data exist
                        if len(rows)>0:
                                for row in rows:
                                        result.append({
                                                'date': row['date'],
                                                'N_person': row['N_person'],
                                                'hours': int(row['hour']),
                                                'pronum': row['KLL']
                                                #'pronum': (random.randint(0, 100))
                                        })

                except Exception as inst:
                        logging.getLogger('error_Logger').error('ConvenienceStore orderhour24_test Query Err')
                        logging.getLogger('error_Logger').error(inst)

                finally:
                        cursor.close()
                        conn.close()

                response = jsonify(result)
                response.status_code=200
                return result
