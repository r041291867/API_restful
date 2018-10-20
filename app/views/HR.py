#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint,current_app, make_response, json
from flask_restful import Resource, Api
from . import views_blueprint
from app.extensions import mongo,mysql,restapi,cache
from MySQLdb import cursors

class HR(Resource):
	def get(self):
		return {'hello': 'world'}
class EmpInfo(Resource) :
	def get(self, emp_no):
		EmpInfo = mongo.db.EMP_INFO_20161212.find_one({'_id':emp_no})
		
		return EmpInfo.get('EMP_NAME',None)
		#print(db['EMP_INFO_20161218'])
		#return {'hello': emp_no}

class EmpInfo2(Resource) :
	def get(self, emp_no):
		cursor = mysql.connection.cursor()
		cursor.execute("SELECT * from HR.EMP_INFO_20161212 where emp_no = '{0}'".format(emp_no))
		data = cursor.fetchone()
		EmpName = data[1]
		
		cursor.close()
		mysql.connection.close()
		return make_response(
				json.dumps({'Emp_Name':EmpName} ,
				ensure_ascii=False))
		#return {'Emp_Name':EmpName} 
		#return {'hello': emp_no}


class EmpInfo3(Resource) :
	def get(self, emp_no, headers=None):
		cursor = mysql.connection.cursor(cursors.DictCursor)
		cursor.execute("SELECT * from HR.EMP_INFO_20161212 where emp_no = '{0}' limit 1".format(emp_no))
		data = cursor.fetchone()
		EmpName = data['emp_name']
		print(emp_no)
		print(EmpName)
		cursor.close()
#		mysql.connection.close()
		resp = make_response(
				json.dumps({'Emp_Name':EmpName} ,
				ensure_ascii=False))
		resp.headers.extend(headers or {})
		return resp


restapi.add_resource(HR, '/HR')
restapi.add_resource(EmpInfo, '/HR/<string:emp_no>')
restapi.add_resource(EmpInfo3, '/HR/MySql/<string:emp_no>')