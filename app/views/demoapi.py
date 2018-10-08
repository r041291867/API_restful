import flask_restful

from flask_restful import request
from werkzeug.datastructures import FileStorage
from flask import Flask
from flask_restful import Resource, Api, reqparse
from app.extensions import mongo,mysql2,restapi,cache
import time
import logging
from datetime import date,datetime as dt, timedelta
import codecs, hashlib, os

app = Flask(__name__)
api = Api(app=app)

@restapi.resource('/data')
class DataApi(Resource):
	def get(self):
		fp = open('/FCH2232J9NJ', "r")
		line = fp.readline()
		while line :
			if (line == '}\n' or line == '}}'): line = fp.readline()
			else :
				line = line.strip('{@\n}')
				sp = line.split("|")
				db_sp = [] 			#要存入資料庫內的list
				while '' in sp: sp.remove('')
				print (sp)
				if (sp[0] == 'BATCH') :
					db_sp.append(sp[1])
					db_sp.append(sp[7])
					db_sp.append(sp[8])
					line = fp.readline()
					sp = line.split("|")
					while '' in sp: sp.remove('')
					db_sp.append(sp[1])
					db_sp.append(sp[2])
					#日期格式轉換
					Btime = dt.strptime('20'+sp[3], '%Y%m%d%H%M%S')     #BeginTime
					Etime = dt.strptime('20'+sp[9], '%Y%m%d%H%M%S')		#EndTime
					db_sp.append(Btime)
					db_sp.append(Etime)
					print (db_sp)
				elif (sp[0] == 'BLOCK') :
					db_sp.append(sp[1])
					db_sp.append(sp[2])
					line = fp.readline()
					while line :		#block內可能有好幾個不同的測試
						if (line == '}\n'): 
							if (db_sp[0] == 'testjet'): 	#testjet的block結束需連續抓到兩個'}'
								line = fp.readline()
								if (line == '}\n'): break
							else : break
						else :
							line = line.strip('{@\n}')
							sp = line.split("{@")
							while '' in sp: sp.remove('')
							lines = []
							for item in sp:
								line = line.strip('{@\n}')
								lines += item.split("|")
							db_sp_new = db_sp + lines
							#print ('test001')
							print (db_sp_new)
							line = fp.readline()
				#else : break
				line = fp.readline()
		fp.close()
		return {'hello': 'world'}
	"""
	数据接口
	"""
	def __init__(self):
		self.parser = reqparse.RequestParser()
		self.parser.add_argument('file', required=True, type=FileStorage, location='files')

	def post(self):
		#now = time.strftime("%Y-%m-%d-%H_%M_%S",time.localtime(time.time()))
		#file = request.files['file']
		result = {"result":"Fail"}
		WriteDbResult = False
		#read file
		fp = open('/FCH2232J9NJ', 'r')
		line = fp.readline()
		insert_batch = [],insert_tjet = []
		while line :
			if (line == '}\n' or line == '}}'): line = fp.readline()	#遇到'}'不處理
			else :
				line = line.strip('{@\n}') #去除外圍的括號與＠
				sp = line.split("|")
				db_sp = [] 			#要存入資料庫內的list
				#去除空字串
				while '' in sp: sp.remove('')
				if (sp[0] == 'BATCH') :
					db_sp.append(sp[1])
					db_sp.append(sp[7])
					db_sp.append(sp[8])
					line = fp.readline()
					sp = line.split("|")
					while '' in sp: sp.remove('')
					db_sp.append(sp[1])
					db_sp.append(sp[2])
					#日期格式轉換
					Btime = dt.strptime('20'+sp[3], '%Y%m%d%H%M%S')     #BeginTime
					Etime = dt.strptime('20'+sp[9], '%Y%m%d%H%M%S')		#EndTime
					db_sp.append(Btime)
					db_sp.append(Etime)
					insert_batch = db_sp
					#print (db_sp)
				elif (sp[0] == 'BLOCK') :
						db_sp.append(sp[1])
						db_sp.append(sp[2])
						line = fp.readline()
						while line :		#block內可能有好幾個不同的測試
							if (line == '}\n'): 
								if (db_sp[0] == 'testjet'): 	#testjet的block結束需連續抓到兩個'}'
									line = fp.readline()
									if (line == '}\n'): break
								else : break
							else :
								line = line.strip('{@\n}')
								sp = line.split("{@")
								while '' in sp: sp.remove('')
								lines = []
								for item in sp:
									line = line.strip('{@\n}')
									lines += item.split("|")
								db_sp_new = db_sp + lines
								#print ('test001')
								print (db_sp_new)
								line = fp.readline()
				#else : break
				line = fp.readline()

		#WriteDbResult = self.WriteToDb(sp)
 		
		Insertsql = 'insert ignore into ICT_Project.ict_result(board,operator,machine,sn,status,start_time,ent_time,log_time) values ('
		for item in insert_batch:
			Insertsql = Insertsql + '"' + str(item) + '"' + ','
		Insertsql = Insertsql.strip(',')
		Insertsql += ')'
		print (Insertsql)
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute(Insertsql)
			conn.commit()
			cursor.close()
			conn.close()
			result['result'] = 'Success'	
		except Exception as inst:
			print('FulearnV4 Test Data MySql Write Err')
			logging.getLogger('error_Logger').error('FulearnV4 Test Data MySql Write Err')
			logging.getLogger('error_Logger').error(inst)
			with codecs.open('./Log/ErrPost/Test_{0}.sql'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb', "utf-8") as ErrOut :
				ErrOut.write(Insertsql)
				ErrOut.write('\n')
				ErrOut.close()
		# print(file.name, file.mimetype, file.stream)
		#file.save('./data/'+now+r'.csv')
		fp.close()

		return result

	def WriteToDb(self,lists,type):
		if (type == 0) :
			Items = 'insert ignore into ICT_Project.ict_result(board,operator,machine,sn,status,start_time,end_time,log_time) values ('
		elif (type == 1) :
			Items = 'insert ignore into ICT_Project.preshort_result(machine,sn,component,status,test_type,measured,high_limit,low_limit,end_time) values ('
		elif (type == 2) :
			Items = 'insert ignore into ICT_Project.open_short_result(machine,sn,status,component,status,end_time) values ('
		elif (type == 3) :
			Items = 'insert ignore into ICT_Project.testjet_result(machine,sn,component,status,device,end_time) values ('
		elif (type == 4) :
			Items = 'insert ignore into ICT_Project.analog_result(machine,sn,component,status,measured,test_type,nominal,high_limit,low_limit,end_time,log_time) values ('
		for item in lists:
			Items = Items + '"' + item + '"' + ','
		Items = Items.strip(',')
		Items += ')'
		#print (Items)
		try :
			conn = mysql2.connect()
			cursor = conn.cursor()
			cursor.execute(Items)
			conn.commit()
			cursor.close()
			conn.close()
			return True	
		except Exception as inst:
			print('FulearnV4 Test Data MySql Write Err')
			logging.getLogger('error_Logger').error('FulearnV4 Test Data MySql Write Err')
			logging.getLogger('error_Logger').error(inst)
			with codecs.open('./Log/ErrPost/Test_{0}.sql'.format(dt.now().strftime('%Y%m%d%H%M%S')),'wb', "utf-8") as ErrOut :
				ErrOut.write(Items)
				ErrOut.write('\n')
				ErrOut.close()
		return False

api.add_resource(DataApi, '/data/')


if __name__ == '__main__':
	app.run(debug=True)