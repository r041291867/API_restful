#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint,current_app, make_response, request
from flask_restful import Resource, Api, reqparse

from . import views_blueprint
from app.extensions import mysql2,restapi,cache,mongo
from app.utils import cache_key
from flask import request
import textwrap
import gzip
import logging
import json
import math
from datetime import date,datetime as dt, timedelta
import codecs, hashlib, os

@restapi.resource('/DataCollect/DeviceCollect')
class DataCollectPostDeviceData(Resource):
	def post(self):
		ContentEncoding = request.headers.get('Content-Encoding').upper()
		result = {'result':'Fail'}
		data = []
		JsonStr = None
		if not ContentEncoding is None and ContentEncoding.find('GZIP') >= 0 :
			try :
				data = gzip.decompress(request.data)
				JsonStr = str(data,encoding='utf8')
				data = json.loads(JsonStr)
			except Exception as inst:
				print('DataCollect Device Post gzip data error')
				print(inst)
				logging.getLogger('error_Logger').error('ACG Post Data Load Json Err')
				logging.getLogger('error_Logger').error(inst)

				open('./Log/ErrDevicePost/ErrPost_{0}.gz'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb').write(request.data)
		else :
			try :
				data = request.get_json(force=True)
			except Exception as inst:
				print('DataCollect Device Post json data error')
				print(inst)
				logging.getLogger('error_Logger').error('ACG Post Data Load Json Err')
				logging.getLogger('error_Logger').error(inst)
				open('./Log/ErrDevicePost/ErrPost_{0}.json'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb').write(request.data)
		if len(data) == 0 :
			return result
		WriteDbResult = False
		try :
			WriteDbResult = self.WriteToDb(data)
		except Exception as inst:
			print('DataCollect Device Write to db Error')
			logging.getLogger('error_Logger').error(inst)
			if not JsonStr is None :
				with codecs.open('./Log/ErrDevicePost/ErrPost_{0}.json'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb', "utf-8") as ErrOut :
					ErrOut.write(JsonStr)
					ErrOut.close()

		if WriteDbResult :
			result['result'] = 'Success'

		return result

	def WriteToDb(self,Rows):
		bulk = mongo.cx['DataCollect'].Device_CollectData_Today.initialize_unordered_bulk_op()
		SqlRows = []
		LastRow = Rows[-1:][0]
		LastDateTime = dt.strptime(LastRow['LogDateTime'],'%Y-%m-%d %H:%M:%S')
		now = dt.now()
		Low = dt(now.year - 1, now.month, now.day, now.hour, now.minute, now.second, now.microsecond)
		High = dt(now.year + 1, now.month, now.day, now.hour, now.minute, now.second, now.microsecond)
		Currection = None
		if LastDateTime < Low or LastDateTime > High:
			Currection = now - LastDateTime

		for Row in Rows:
			fs = ''
			if Row.get('FunctionStack') != None:
#				fs = '|'.join([item if isinstance(item, str) else '|'.join(item) for item in Row.get('FunctionStack') ])
				FunctionStack = []
				for item in Row.get('FunctionStack') :
					if not item is None and isinstance(item, str):
						FunctionStack.append(item)
				fs = '|'.join(FunctionStack)
			Item = ''
			if Row.get('Item') != None and len(Row['Item'].keys()) > 0:
				Item = 'COLUMN_CREATE('
				for k in Row['Item'].keys():
					if type(Row['Item'][k]) == list:
						ls = ''
						for c in Row['Item'][k]:
							ls = ls + c + '|'
						Item = Item + '\'{0}\',\'{1}\''.format(str(k),ls[:-1])
					elif type(Row['Item'][k]) != str:
						Item = Item + '\'{0}\',{1}'.format(str(k),str(Row['Item']['Item'][k]))
					else:
						Item = Item + '\'{0}\',\'{1}\''.format(str(k).replace("'","\'"),str(Row['Item'][k]))
					Item = Item + ','
				Item = Item[:-1] + ')'
			else :
				Item = ''

			if Row.get('LogDateTime') != None and Currection != None :

				NewLogDataTime = dt.strptime( Row['LogDateTime'],'%Y-%m-%d %H:%M:%S') + Currection
				Row['LogDateTime'] = NewLogDataTime.strftime('%Y-%m-%d %H:%M:%S')

			oneSqlRow = [
				'null' if Row.get('LogDateTime') == None else '\'{0}\''.format(Row.get('LogDateTime'))
				,'null' if Row.get('TrackId') == None else '\'{0}\''.format(Row.get('TrackId'))
				,'null' if Row.get('UserId') == None else '\'{0}\''.format(Row.get('UserId'))
				,'null' if Row.get('UUID') == None else '\'{0}\''.format(Row.get('UUID'))
				,'null' if Row.get('OS') == None else '\'{0}\''.format(Row.get('OS'))
				,'null' if Row.get('DeviceKind') == None else '\'{0}\''.format(Row.get('DeviceKind'))
				,'null' if Row.get('IP') == None else '\'{0}\''.format(Row.get('IP'))
				,'null' if Row.get('GPS').get('Lon') == None else str(Row.get('GPS').get('Lon'))
				,'null' if Row.get('GPS').get('Lat') == None else str(Row.get('GPS').get('Lat'))
				,'null' if Row.get('Action').get('ActionKind') == None else '\'{0}\''.format(Row.get('Action').get('ActionKind'))
				,'null' if Row.get('Action').get('Action') == None else '\'{0}\''.format(Row.get('Action').get('Action'))
				,'null' if Row.get('TapPoint').get('X') == None else str(Row.get('TapPoint').get('X'))
				,'null' if Row.get('TapPoint').get('Y') == None else str(Row.get('TapPoint').get('Y'))
				,'null' if fs == '' else '\'{0}\''.format(fs)
			]
			md5_text = hashlib.md5()
			md5_text.update('_'.join(oneSqlRow).encode('utf8'))
			oneSqlRow.append('null' if Item == '' else Item)
			oneSqlRow.append('null' if md5_text.hexdigest() == '' else '\'{0}\''.format(md5_text.hexdigest()))
			Row['CreateDateTime'] = now.strftime('%Y-%m-%d %H:%M:%S')
			Row['_id'] = md5_text.hexdigest()
			bulk.insert(Row)
			oneRowSql = '('+(','.join(oneSqlRow))+')'
			SqlRows.append(oneRowSql)
		InsertSql = textwrap.dedent("""replace into DataCollect.Device_CollectData_Today(LogDateTime,TrackId,UserId,UUID,OS,DeviceKind,IP,Lon,Lat,ActionKind,Action,X,Y,FunctionStack,Item,Md5) values {0};
		""").format(','.join(SqlRows))
		AfterSql = "call DataCollect.SP_AfterInsertDevice_CollectData();"
		try :
			bulk.execute()
		except Exception as inst:
			print('DataCollect Device Post Mongo Write Err')
			print(inst)
			logging.getLogger('error_Logger').error('DataCollect Device Post Mongo Write Err')
			logging.getLogger('error_Logger').error(inst)
#			logging.error(inst)
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute(InsertSql)
			cursor.execute(AfterSql)
			conn.commit()
			cursor.close()
			conn.close()
			return True
		except Exception as inst:
			print('DataCollect Device Post Data MySql Write Err')
			logging.getLogger('error_Logger').error('DataCollect Device Post Data MySql Write Err')
			logging.getLogger('error_Logger').error(inst)
			with codecs.open('./Log/ErrDevicePost/Output_{0}.sql'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb', "utf-8") as ErrOut :
#				ErrOut.write(TruncateSql)
#				ErrOut.write('\n')
				ErrOut.write(InsertSql)
				ErrOut.write('\n')
				ErrOut.write(AfterSql)
				ErrOut.close()
		return False

@restapi.resource('/DataCollect/WebCollect')
class DataCollectPostWebData(Resource):
	def get(self) :
		result = {'result':'GET Fail'}
		parser = reqparse.RequestParser()
		parser.add_argument('v', type=str)
		parser.add_argument('tid', type=str)
		parser.add_argument('cid', type=str)
		parser.add_argument('t', type=str)
		parser.add_argument('dl', type=str)
		parser.add_argument('dt', type=str)
		parser.add_argument('vp', type=str)
		parser.add_argument('sr', type=str)
		parser.add_argument('hr', type=str)
		parser.add_argument('ul', type=str)
		parser.add_argument('z', type=str)
		parser.add_argument('dr', type=str)
		parser.add_argument('ec', type=str)
		parser.add_argument('ea', type=str)
		parser.add_argument('el', type=str)
		parser.add_argument('ev', type=str)
		args = parser.parse_args()
		WriteObj = {"Version":args['v']
			,"TrackId":args['tid']
			,"UUID":args['cid']
			,"EventKind":args['t']
			,"PathLocation":args['dl']
			,"Href":args['hr']
			,"WebTitle":args['dt']
			,"WidthHeight":args['vp']
			,"ScreenWidthHeight":args['sr']
			,"Language":args['ul']
			,"Z":args['z']
			,"Referrer":args['dr']
			,"EventCategory":args['ec']
			,"EventAction":args['ea']
			,"EventLabel":args['el']
			,"EventValue":args['ev']
			,"UserAgent":request.headers.get('User-Agent')##ua
			,"IP":request.headers.get('X-Forwarded-For').split(",")[0] ##IP
		}
		WriteDbResult = False
		try :
			if WriteObj['Version'] and WriteObj['TrackId'] and WriteObj['UUID'] and WriteObj['Z'] :
				WriteDbResult = self.WriteToDb(WriteObj)
		except Exception as inst:
			print('DataCollect Web Get Write to db Error')
			logging.getLogger('error_Logger').error(inst)
		if WriteDbResult :
			result['result'] = 'GET Success'

		return result
	def post(self) :
		if request.headers.get('Content-Encoding') == None:
			ContentEncoding = None
		else:
			ContentEncoding = request.headers.get('Content-Encoding').upper()
		result = {'result':'POST Fail'}
		rst = make_response(str(result))
		rst.headers['Access-Control-Allow-Origin'] = '*'
		data = []
		JsonStr = None
		if not ContentEncoding is None and ContentEncoding.find('GZIP') >= 0 :
			try :
				data = gzip.decompress(request.data)
				JsonStr = str(data,encoding='utf8')
				data = json.loads(JsonStr)
			except Exception as inst:
				print('DataCollect Web Post gzip data error')
				print(inst)
				logging.getLogger('error_Logger').error('ACG Web Post Data Load Json Err')
				logging.getLogger('error_Logger').error(inst)

				open('./Log/ErrDevicePost/ErrPost_{0}.gz'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb').write(request.data)
		else :
			try :
				data = request.get_json(force=True)
			except Exception as inst:
				print('DataCollect Web Post json data error')
				print(inst)
				logging.getLogger('error_Logger').error('ACG Web Post Data Load Json Err')
				logging.getLogger('error_Logger').error(inst)
				open('./Log/ErrDevicePost/ErrPost_{0}.json'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb').write(request.data)
		if len(data) == 0 :
			return rst
#		keys = request.form.keys() or['']
		keys = list(data.keys())
		if (not 'v' in keys) or (not 'tid' in keys) or (not 'cid' in keys) or (not 't' in keys)\
		or (not 'dl' in keys) or (not 'dt' in keys) or (not 'vp' in keys) or (not 'sr' in keys)\
		or (not 'ul' in keys) or (not 'z' in keys) or (not 'hr' in keys):
			return rst
		# print('v' in keys)
		# print(request.form['v'])
#		print('Create WriteObj')
		WriteObj = {"Version": None
			,"TrackId": None
			,"UUID": None
			,"EventKind": None
			,"PathLocation": None
			,"Href": None
			,"WebTitle": None
			,"WidthHeight": None
			,"ScreenWidthHeight": None
			,"Language": None
			,"Z": None
			,"Referrer": None
			,"EventCategory": None
			,"EventAction": None
			,"EventLabel": None
			,"EventValue": None
			,"UserAgent":request.headers.get('User-Agent')##ua
			,"IP":request.headers.get('X-Forwarded-For').split(",")[0] ##IP
		}
		if 'v' in keys :
#			print('v' in keys)
#			print(list(keys))
			WriteObj["Version"] = data['v']
		if 'tid' in keys :
#			print('tid' in keys)
			WriteObj["TrackId"] = data['tid']
		if 'cid' in keys :
			WriteObj["UUID"] = data['cid']
		if 't' in keys :
			WriteObj["EventKind"] = data['t']
		if 'dl' in keys :
			WriteObj["PathLocation"] = data['dl']
		if 'hr' in keys :
			WriteObj["Href"] = data['hr']
		if 'dt' in keys :
			WriteObj["WebTitle"] = data['dt']
		if 'vp' in keys :
			WriteObj["WidthHeight"] = data['vp']
		if 'sr' in keys :
			WriteObj["ScreenWidthHeight"] = data['sr']
		if 'ul' in keys :
			WriteObj["Language"] = data['ul']
		if 'z' in keys :
			WriteObj["Z"] = data['z']
		if 'dr' in keys :
			WriteObj["Referrer"] = data['dr']
		if 'ec' in keys :
			WriteObj["EventCategory"] = data['ec']
		if 'ea' in keys :
			WriteObj["EventAction"] = data['ea']
		if 'el' in keys :
			WriteObj["EventLabel"] = data['el']
		if 'ev' in keys :
			WriteObj["EventValue"] = data['ev']

#		print(WriteObj)
		WriteDbResult = False
#		print('Start WriteToDb')
		try :
			WriteDbResult = self.WriteToDb(WriteObj)
		except Exception as inst:
			print('DataCollect Web Write to db Error')
			# logging.getLogger('error_Logger').error(inst)
		if WriteDbResult :
			result['result'] = 'POST Success'
		rst = make_response(str(result))
		rst.headers['Access-Control-Allow-Origin'] = '*'
		return rst


	def WriteToDb(self,Obj) :
#		print('WriteToDb')
		now = dt.now()
		Obj["CreateDateTime"] = now.strftime('%Y-%m-%d %H:%M:%S')
		try :
			print(mongo.db.name)
			mongo.cx['DataCollect'].Web_CollectData_Today.insert_one(Obj)
		except Exception as inst:
			print('DataCollect WebCollect Post Data MongoDB Write Err')
			logging.getLogger('error_Logger').error('DataCollect WebCollect Post Data MongoDB Write Err')
			logging.getLogger('error_Logger').error(inst)
		return True
		Sql = 'replace into DataCollect.Web_CollectData_Today(Version,TrackId,UUID,EventKind,PathLocation,WebTitle,WidthHeight,ScreenWidthHeight,Language,Z,Referrer,EventCategory,EventAction,EventLabel,EventValue,UserAgent,IP,CreateDateTime) values ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17})'.format(
			'null' if Obj['Version'] is None else '\'{0}\''.format(Obj['Version'])
			,'null' if Obj['TrackId'] is None else '\'{0}\''.format(Obj['TrackId'])
			,'null' if Obj['UUID'] is None else '\'{0}\''.format(Obj['UUID'])
			,'null' if Obj['EventKind'] is None else '\'{0}\''.format(Obj['EventKind'])
			,'null' if Obj['PathLocation'] is None else '\'{0}\''.format(Obj['PathLocation'])
			,'null' if Obj['WebTitle'] is None else '\'{0}\''.format(Obj['WebTitle'])
			,'null' if Obj['WidthHeight'] is None else '\'{0}\''.format(Obj['WidthHeight'])
			,'null' if Obj['ScreenWidthHeight'] is None else '\'{0}\''.format(Obj['ScreenWidthHeight'])
			,'null' if Obj['Language'] is None else '\'{0}\''.format(Obj['Language'])
			,'null' if Obj['Z'] is None else '\'{0}\''.format(Obj['Z'])
			,'null' if Obj['Referrer'] is None else '\'{0}\''.format(Obj['Referrer'])
			,'null' if Obj['EventCategory'] is None else '\'{0}\''.format(Obj['EventCategory'])
			,'null' if Obj['EventAction'] is None else '\'{0}\''.format(Obj['EventAction'])
			,'null' if Obj['EventLabel'] is None else '\'{0}\''.format(Obj['EventLabel'])
			,'null' if Obj['EventValue'] is None else '\'{0}\''.format(Obj['EventValue'])
			,'null' if Obj['UserAgent'] is None else '\'{0}\''.format(Obj['UserAgent'])
			,'null' if Obj['IP'] is None else '\'{0}\''.format(Obj['IP'])
			,'null' if Obj['CreateDateTime'] is None else '\'{0}\''.format(Obj['CreateDateTime'])
		)
		AfterSql = 'call DataCollect.SP_AfterInsertWeb_CollectData()'
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute(Sql)
			cursor.execute(AfterSql)
			conn.commit()
			cursor.close()
			conn.close()
			return True
		except Exception as inst:
			print('DataCollect WebCollect Post Data MySql Write Err')
			logging.getLogger('error_Logger').error('DataCollect WebCollect Post Data MySql Write Err')
			logging.getLogger('error_Logger').error(inst)
			with codecs.open('./Log/ErrWebDataPost/Output_{0}.sql'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb', "utf-8") as ErrOut :
				ErrOut.write(Sql)
				ErrOut.write('\n')
				ErrOut.write(AfterSql)
				ErrOut.close()
		return False