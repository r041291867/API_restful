#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint,current_app, make_response, request
from flask_restful import Resource, Api

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
import codecs


@restapi.resource('/fulearn/V4/data')
class FulearnV4PostData(Resource):
	def post(self):
		ContentEncoding = request.headers.get('Content-Encoding')
		result = {'result':'Fail'}
		fulearndata = []
		JsonStr = None
		if not ContentEncoding is None and ContentEncoding.find('gzip') >= 0 :
			try :
				data = gzip.decompress(request.data)
				JsonStr = str(data,encoding='utf8')
				fulearndata = json.loads(JsonStr)
			except Exception as inst:
				print('Post gzip data error')
				print(inst)
				logging.getLogger('error_Logger').error('FulearnV4 Post Data Load Json Err')
				logging.getLogger('error_Logger').error(inst)
				
				open('./Log/ErrPost/ErrPost_{0}.gz'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb').write(request.data)
				
		else :
			try :
				fulearndata = request.get_json(force=True)
			except Exception as inst:
				print('Post json data error')
				print(inst)
				logging.getLogger('error_Logger').error('FulearnV4 Post Data Load Json Err')
				logging.getLogger('error_Logger').error(inst)
				open('./Log/ErrPost/ErrPost_{0}.json'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb').write(request.data)
#			JsonStr = json.dumps(fulearndata, ensure_ascii=False)
		if len(fulearndata) == 0 :
			return result
		WriteDbResult = False
		try :
			WriteDbResult = self.WriteToDb(fulearndata)
		except Exception as inst:
			print('Write to db Error')
			logging.getLogger('error_Logger').error(inst)
			if not JsonStr is None :
				with codecs.open('./Log/ErrPost/ErrPost_{0}.json'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb', "utf-8") as ErrOut :
					ErrOut.write(JsonStr)
					ErrOut.close()
#		print(WriteDbResult)
		
		if WriteDbResult :
			result['result'] = 'Success'
		
		return result

	def WriteToDb(self,Rows):
		bulk = mongo.cx['fulearn_4'].Fulearn_Data_V4.initialize_unordered_bulk_op()
		SqlRows = []
		LastRow = Rows[-1:][0]
		LastDateTime = dt.strptime( LastRow['LogDateTime'],'%Y-%m-%d %H:%M:%S')
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
				,'null' if Item == '' else Item
			]
			Row['CreateDateTime'] = now.strftime('%Y-%m-%d %H:%M:%S')
			bulk.insert(Row)
			oneRowSql = '('+(','.join(oneSqlRow))+')'
			SqlRows.append(oneRowSql)
#		TruncateSql = "truncate table fulearn_4.Fulearn_Data_V4_Today;"
		InsertSql = textwrap.dedent("""
insert ignore into fulearn_4.Fulearn_Data_V4_Today(LogDateTime,UserId,UUID,OS,DeviceKind,IP,Lon,Lat,ActionKind,Action,X,Y,FunctionStack,Item) values 
{0};
		""").format(','.join(SqlRows))
		AfterSql = "call fulearn_4.SP_AfterInsertFulearnDataV4();"
		#print(Sql)
		try :
			bulk.execute()
		except Exception as inst:
			print('FulearnV4 Post Data Mongo Write Err')
			print(inst)
			logging.getLogger('error_Logger').error('FulearnV4 Post Data Mongo Write Err')
			logging.getLogger('error_Logger').error(inst)
#			logging.error(inst)
		try :
#			print("mysql conn")
			conn = mysql2.connect()
#			conn.open()
#			print("mysql cursor")
			cursor = conn.cursor()
#			print("mysql execute")
#			cursor.execute(TruncateSql)
			cursor.execute(InsertSql)
			cursor.execute(AfterSql)
#			print("mysql commit")
			conn.commit()
#			print("mysql cursor close")
			cursor.close()
#			print("mysql conn close")
			conn.close()
			return True
		except Exception as inst:
			print('FulearnV4 Post Data MySql Write Err')
			logging.getLogger('error_Logger').error('FulearnV4 Post Data MySql Write Err')
			logging.getLogger('error_Logger').error(inst)
			with codecs.open('./Log/ErrPost/Output_{0}.sql'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb', "utf-8") as ErrOut :
#				ErrOut.write(TruncateSql)
#				ErrOut.write('\n')
				ErrOut.write(InsertSql)
				ErrOut.write('\n')
				ErrOut.write(AfterSql)
				ErrOut.close()
		return False
		
@restapi.resource('/fulearn/V4/WebCollect')
class FulearnV4WebPost(Resource):
	def get(self) :
		result = {'result':'Fail'}
		parser = reqparse.RequestParser()
		parser.add_argument('v', type=str)
		parser.add_argument('tid', type=str)
		parser.add_argument('cid', type=str)
		parser.add_argument('t', type=str)
		parser.add_argument('dl', type=str)
		parser.add_argument('dt', type=str)
		parser.add_argument('vp', type=str)
		parser.add_argument('sr', type=str)
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
			,"IP":request.remote_addr ##IP
		}
		WriteDbResult = False
		try :
			if WriteObj['Version'] and WriteObj['TrackId'] and WriteObj['UUID'] and WriteObj['Z'] :
				WriteDbResult = self.WriteToDb(WriteObj)
		except Exception as inst:
			print('Write to db Error')
			logging.getLogger('error_Logger').error(inst)
		if WriteDbResult :
			result['result'] = 'Success'
		
		return result
	def post(self) :
		result = {'result':'Fail'}
#		print(request.form['v'])
#		keys = request.form.keys() or['']
		keys = list(request.form.keys())
#		print(list(keys))
		if (not 'v' in keys) or (not 'tid' in keys) or (not 'cid' in keys) or (not 't' in keys)\
		or (not 'dl' in keys) or (not 'dt' in keys) or (not 'vp' in keys) or (not 'sr' in keys)\
		or (not 'ul' in keys) or (not 'z' in keys):
			return result
		# print('v' in keys)
		# print(request.form['v'])
		print('Create WriteObj')
		WriteObj = {"Version": None
			,"TrackId": None
			,"UUID": None
			,"EventKind": None
			,"PathLocation": None
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
			,"IP":request.remote_addr ##IP
		}
		if 'v' in keys :
#			print('v' in keys)
#			print(list(keys))
			WriteObj["Version"] = request.form['v']
		if 'tid' in keys :
#			print('tid' in keys)
			WriteObj["TrackId"] = request.form['tid']
		if 'cid' in keys :
			WriteObj["UUID"] = request.form['cid']
		if 't' in keys :
			WriteObj["EventKind"] = request.form['t']
		if 'dl' in keys :
			WriteObj["PathLocation"] = request.form['dl']
		if 'dt' in keys :
			WriteObj["WebTitle"] = request.form['dt']
		if 'vp' in keys :
			WriteObj["WidthHeight"] = request.form['vp']
		if 'sr' in keys :
			WriteObj["ScreenWidthHeight"] = request.form['sr']
		if 'ul' in keys :
			WriteObj["Language"] = request.form['ul']
		if 'z' in keys :
			WriteObj["Z"] = request.form['z']
		if 'dr' in keys :
			WriteObj["Referrer"] = request.form['dr']
		if 'ec' in keys :
			WriteObj["EventCategory"] = request.form['ec']
		if 'ea' in keys :
			WriteObj["EventAction"] = request.form['ea']
		if 'el' in keys :
			WriteObj["EventLabel"] = request.form['el']
		if 'ev' in keys :
			WriteObj["EventValue"] = request.form['ev']
		
#		print(WriteObj)
		WriteDbResult = False
#		print('Start WriteToDb')
		try :
			WriteDbResult = self.WriteToDb(WriteObj)
		except Exception as inst:
			print('Write to db Error')
			# logging.getLogger('error_Logger').error(inst)
		if WriteDbResult :
			result['result'] = 'Success'
		
		return result
	

	def WriteToDb(self,Obj) :
#		print('WriteToDb')
		now = dt.now()
		Obj["CreateDateTime"] = now.strftime('%Y-%m-%d %H:%M:%S')
		try :
			mongo.cx['fulearn_4'].FulearnWebCollect.insert_one(Obj)
		except Exception as inst:
			print('FulearnV4 WebCollect Post Data MongoDB Write Err')
			# logging.getLogger('error_Logger').error('FulearnV4 WebCollect Post Data MongoDB Write Err')
			# logging.getLogger('error_Logger').error(inst)
		Sql = 'insert ignore into fulearn_4.Fulearn_WebCollect_Today(Version,TrackId,UUID,EventKind,PathLocation,WebTitle,WidthHeight,ScreenWidthHeight,Language,Z,Referrer,EventCategory,EventAction,EventLabel,EventValue,UserAgent,IP,CreateDateTime) values ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17})'.format(
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
		AfterSql = 'call fulearn_4.SP_AfterInsertFulearWebCollect()'
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
			print('FulearnV4 WebCollect Post Data MySql Write Err')
			# logging.getLogger('error_Logger').error('FulearnV4 WebCollect Post Data MySql Write Err')
			# logging.getLogger('error_Logger').error(inst)
			with codecs.open('./Log/ErrPost/Output_{0}.sql'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb', "utf-8") as ErrOut :
				ErrOut.write(Sql)
				ErrOut.write('\n')
				ErrOut.write(AfterSql)
				ErrOut.close()
		return False
		
