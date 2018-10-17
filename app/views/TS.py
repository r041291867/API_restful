import flask_restful

from flask_restful import request
from werkzeug.datastructures import FileStorage
from flask import Flask
from flask_restful import Resource, Api, reqparse
from app.extensions import mongo,mysql2,restapi,cache
import time
import logging
from datetime import date,datetime as dt, timedelta
import codecs, hashlib, os, shutil


app = Flask(__name__)
api = Api(app=app)

@restapi.resource('/data')
class DataApi(Resource):
	def get(self):
		#解析所有fulllog中的log資料
		path=os.path.abspath("..")+"fulllog/"
		# for f in os.listdir(path):	
		fp = open('/processedlog/FCH2232JA5Q180829010926.txt', 'r')
		# fp = open(path+f, "r")
		# print(path+f)
		line = fp.readline()
		machine = ''
		sn_code = ''		#儲存機台編號及sn碼
		EndTime = ''
		while line :
			if (line == '}\n' or line == '}}'): line = fp.readline()
			else :
				line = line.strip('{@\n}')
				sp = line.split("|")
				db_sp = [] 	 		#要存入資料庫內的list
				
				while '' in sp: sp.remove('')
				# print (sp)
				#處理要寫入ict_result資料
				if (sp[0] == 'BATCH') :
					db_sp.append(sp[1])
					db_sp.append(sp[7])
					db_sp.append(sp[8])
					logtime = sp[6]
					machine = sp[8]
					line = fp.readline()
					sp = line.split("|")
					while '' in sp: sp.remove('')
					sn_code = sp[1]
					db_sp.append(sp[1])
					db_sp.append(sp[2])
					#日期格式轉換
					Btime = dt.strptime('20'+sp[3], '%Y%m%d%H%M%S')     #BeginTime
					Etime = dt.strptime('20'+sp[9], '%Y%m%d%H%M%S')		#EndTime
					Ltime = dt.strptime('20'+logtime, '%Y%m%d%H%M%S')   #LogTime
					EndTime = Etime
					db_sp.append(Btime)
					db_sp.append(Etime)
					db_sp.append(Ltime)
					print (db_sp)
				#處理各測試結果資料(pre-short、open/short、testjet、analog、poweron、digital、boundary scan、analog powered、frequency、programming)
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
							line = line.strip('{@\n}') #移除頭尾@\n
							sp = line.split("{@")
							while '' in sp: sp.remove('')
							lines = []
							for item in sp:
								line = line.strip('{@\n}')
								lines += item.split("|")
							db_sp_new = db_sp + lines
							db_sp_new.insert(0,machine)
							db_sp_new.insert(1,sn_code)
							db_sp_new.append(EndTime)

							if (db_sp_new[4] == 'A-JUM') :
								del db_sp_new[3]		#刪除不必要元素
								del db_sp_new[3]
								print (db_sp_new)
								#WriteDbResult = self.WriteToDb(db_sp_new,1)
							elif (db_sp_new[4] == 'TS'):
								del db_sp_new[3]		#刪除不必要元素
								del db_sp_new[3]
								print (db_sp_new)
								#WriteDbResult = self.WriteToDb(db_sp_new,2)
							elif (db_sp_new[4] == 'TJET'):
								del db_sp_new[3]		#刪除不必要元素
								del db_sp_new[3]
								del db_sp_new[4]
								print (db_sp_new)
								#WriteDbResult = self.WriteToDb(db_sp_new,3)
							elif (db_sp_new[4] == 'A-CAP' or db_sp_new[4] == 'A-RES' or db_sp_new[4] == 'A-MEA'):
								if(db_sp_new[4] == 'A-CAP'):
									del db_sp_new[3]		#刪除不必要元素
									db_sp_new.insert(6,'')  #A-CAP沒有test_condition
								elif(db_sp_new[4] == 'A-RES'):
									del db_sp_new[3]		#刪除不必要元素
									db_sp_new.insert(6,'')  #A-RES沒有test_condition
								elif(db_sp_new[4] == 'A-MEA'):
									del db_sp_new[3]
									db_sp_new.insert(8,None) #A-MEA沒有nominal
							print (db_sp_new)
							line = fp.readline()
				elif (sp[0] == 'TS') :
					db_sp_new = [machine,sn_code,sp[1],sp[5],EndTime]
					print (db_sp_new)

					#若測試狀態為失敗,需parsing fail report
					if (sp[1]=='1'):
						line=fp.readline()
						while line:
							if (line == '}\n'): break
				
							else:
								#移除頭尾@\n
								line = line.strip('{@\n}') 
								#split |
								sp = line.split("|")
								db_sp = [] 
								db_sp = sp[1].split()

								#陣列第一個元素為RPT且第二個元素用空白切割後陣列長度大於0
								if (sp[0]=="RPT" and len(db_sp)>0):
									#陣列第一個元素為Short或Open
									if (db_sp[0]=="Short" or db_sp[0]=="Open"):
										db_sp_new = [machine,sn_code,EndTime]
										#取得第fail_type為Short/Open
										db_sp_new.append(db_sp[0])
										#取得第二個元素並移除#字號後取得fail_no
										db_sp_new.append(db_sp[1].replace('#',''))
					 		
									#From為fail point起點 To為fail point終點
									elif(db_sp[0]=="From:" or db_sp[0]=="To:"):
										#若遇到針點為v需取下一行才是針點名稱
										if(db_sp[1]=="v"):
											line=fp.readline()
											point = (line.split())[0].split("|")
											db_sp_new.append(point[1])
										else :
											db_sp_new.append(db_sp[1])
										db_sp_new.append(db_sp[2])

										# ohms
										if(len(db_sp)>3 and db_sp[3]!="Open"):
											db_sp_new.append(db_sp[3])
										else:db_sp_new.append(None)
												
									#讀到Common視為其中一項fail結束
									elif (db_sp[0]=="Common"):print(db_sp_new)
										
									#fail Report結束
									elif ("End" in db_sp[0]):break

									line=fp.readline()

								else : line=fp.readline()	        

				line = fp.readline()
		fp.close()
		return {'hello': 'world'}
	"""
	数据接口
	"""
	def __init__(self):
		self.parser = reqparse.RequestParser()
		self.parser.add_argument('file', required=True, type=FileStorage, location='files')

	def post(self,machine,sn_code,EndTime,sp[]):

		db_sp_new = [machine,sn_code,sp[1],EndTime]
		print(db_sp_new)
		#若測試狀態為失敗,需parsing fail report
		if (sp[1]=='1'):
			line=fp.readline()
			while line:
				if (line == '}\n'): break #遇到'}'不處理

				else:
					#移除頭尾@\n split |
					line = line.strip('{@\n}') 
					sp = line.split("|") 									
					db_sp = [] 
					db_sp = sp[1].split()

					#陣列第一個元素為RPT且第二個元素用空白切割後陣列長度大於0
					if (sp[0]=="RPT" and len(db_sp)>0):

						#陣列第一個元素為Short或Open
						if (db_sp[0]=="Short" or db_sp[0]=="Open"):
							db_sp_new = [machine,sn_code,EndTime]
							#取得第fail_type為Short/Open
							db_sp_new.append(db_sp[0])
							#取得第二個元素並移除#字號後取得fail_no
							db_sp_new.append(db_sp[1].replace('#',''))
		 		
						#From為fail point起點 To為fail point終點
						elif(db_sp[0]=="From:" or db_sp[0]=="To:"):
							#若遇到針點為v需取下一行才是針點名稱
							if(db_sp[1]=="v"):
								line=fp.readline()
								point = (line.split())[0].split("|")
								
								db_sp_new.append(point[1])
							else :
								db_sp_new.append(db_sp[1])
							db_sp_new.append(db_sp[2])

							# ohms
							if(len(db_sp)>3 and db_sp[3]!="Open"):
								db_sp_new.append(db_sp[3])
							else:db_sp_new.append(None)
									
						#讀到Common視為其中一項fail結束
						elif (db_sp[0]=="Common"):
							print(db_sp_new)
							
						#fail Report結束
						elif ("End" in db_sp[0]): break

						line=fp.readline()

					else : line=fp.readline()	        

		line = fp.readline()

	

api.add_resource(DataApi, '/data/')


if __name__ == '__main__':
	app.run(debug=True)