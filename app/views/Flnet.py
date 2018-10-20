#!/usr/bin/python
# -*- coding:utf-8 -*-
from flask import Blueprint,current_app, request
from flask_restful import Resource, Api

from . import views_blueprint
from app.extensions import mongo,mysql,restapi,cache
from app.utils import cache_key
from flask import request
import textwrap
import gzip
import logging
import json
from kafka import KafkaProducer
import os
#from manage import app


@restapi.resource('/Flnet')
#@cache.cached(timeout=600, key_prefix='Hello', unless=None)
class Flnet(Resource):
	def get(self):
		return {'hello': 'world'}

# @restapi.resource('/Flnet/Order')
# class PostOrder(Resource):
	# def post(self):
		# ContentEncoding = request.headers.get('Content-Encoding')
		# result = {'result':'Fail'}
		# OrderRecords = []
		# if not ContentEncoding is None and ContentEncoding.find('gzip') >= 0 :
			# JsonData = {}
			# try :
				# data = gzip.decompress(request.data)
				# JsonStr = str(data,encoding='utf8')
				# JsonData = json.loads(JsonStr)
			# except Exception as inst:
				# print(inst)
				# logging.error(init)
			# if not JsonData.get('Password') is None or JsonData['Password'] == 'Flnet_BigData' :
				# OrderRecords = JsonData.get('OrderRecords')
		# else :
			# OrderRecords = request.get_json(force=True)
		# if len(OrderRecords) == 0 :
			# return result
		# WriteDbResult = self.WriteToDb(OrderRecords)
		# if WriteDbResult :
			# result['result'] = 'Success'
		
		# return result
		
	# def WriteToDb(self,Rows) :
		# bulk = mongo.cx['flnet'].OrderRecord.initialize_unordered_bulk_op()
		# SqlRows = []
		# for Row in Rows :
			# PromoterID = Row.get('PromoterID')
			# OrderID = Row.get('OrderID')
			# GoodsID = Row.get('GoodsID')
			# if PromoterID != None and OrderID != None and GoodsID != None:
				# oneSqlRow = [
					# str(Row.get('PromoterID'))
					# ,'null' if Row.get('InOutWarehouseDate') == None else '\'{0}\''.format(Row.get('InOutWarehouseDate'))
					# ,'null' if Row.get('SellOrReturn') == None else str(Row.get('SellOrReturn'))
					# ,'\'{0}\''.format(str(Row.get('OrderID')))
					# ,'\'{0}\''.format(Row.get('GoodsID'))
					# ,'null' if Row.get('GoodsFeeCode') == None else '\'{0}\''.format(Row.get('GoodsFeeCode'))
					# ,'null' if Row.get('GoodsManager') == None else '\'{0}\''.format(Row.get('GoodsManager'))
					# ,'null' if Row.get('GoodsManagerBG') == None else '\'{0}\''.format(Row.get('GoodsManagerBG'))
					# ,'null' if Row.get('NotTaxedGoodsPrice') == None else str(Row.get('NotTaxedGoodsPrice'))
					# ,'null' if Row.get('NotTaxedGoodsAvgPrice') == None else str(Row.get('NotTaxedGoodsAvgPrice'))
					# ,'null' if Row.get('SalesCount') == None else str(Row.get('SalesCount'))
					# ,'null' if Row.get('ReturnCount') == None else str(Row.get('ReturnCount'))
					# ,'null' if Row.get('OrderSalesAmount') == None else str(Row.get('OrderSalesAmount'))
					# ,'null' if Row.get('OrderReturnAmount') == None else str(Row.get('OrderReturnAmount'))
					# ,'null' if Row.get('SalesIncome') == None else str(Row.get('SalesIncome'))
					# ,'null' if Row.get('GoodsAvgCost') == None else str(Row.get('GoodsAvgCost'))
					# ,'null' if Row.get('GoodsSumCost') == None else str(Row.get('GoodsSumCost'))
					# ,'null' if Row.get('GoodsGrossAmount') == None else str(Row.get('GoodsGrossAmount'))
					# ,'null' if Row.get('DiscountSumAmount') == None else str(Row.get('DiscountSumAmount'))
					# ,'null' if Row.get('GoodsDiscount') == None else str(Row.get('GoodsDiscount'))
					# ,'null' if Row.get('ScoreDiscount') == None else str(Row.get('ScoreDiscount'))
					# ,'null' if Row.get('CouponDiscount') == None else str(Row.get('CouponDiscount'))
					# ,'null' if Row.get('ExpessIncome') == None else str(Row.get('ExpessIncome'))
					# ,'null' if Row.get('GiftCardAmount') == None else str(Row.get('GiftCardAmount'))
					# ,'null' if Row.get('OrderFrom') == None else '\'{0}\''.format(Row.get('OrderFrom'))
					# ,'null' if Row.get('DCName') == None else '\'{0}\''.format(Row.get('DCName'))
					# ,'null' if Row.get('SalesCorporation') == None else '\'{0}\''.format(Row.get('SalesCorporation'))
					# ,'null' if Row.get('InOutWarehouseID') == None else '\'{0}\''.format(Row.get('InOutWarehouseID'))
					# ,'null' if Row.get('WarehouseCode') == None else str(Row.get('WarehouseCode'))
					# ,'null' if Row.get('OrderFromProvince') == None else '\'{0}\''.format(Row.get('OrderFromProvince'))
					# ,'null' if Row.get('OrderFromCity') == None else '\'{0}\''.format(Row.get('OrderFromCity'))
					# ,'null' if Row.get('ReceiveProvince') == None else '\'{0}\''.format(Row.get('ReceiveProvince'))
					# ,'null' if Row.get('ReceiveCity') == None else '\'{0}\''.format(Row.get('ReceiveCity'))
					# ,'null' if Row.get('OrderLiability') == None else '\'{0}\''.format(Row.get('OrderLiability'))
					# ,'null' if Row.get('SalesLiability') == None else '\'{0}\''.format(Row.get('SalesLiability'))
					# ,'null' if Row.get('OrderFromIP') == None else '\'{0}\''.format(Row.get('OrderFromIP'))
					# ,'null' if Row.get('CompanyStoreID') == None else '\'{0}\''.format(Row.get('CompanyStoreID'))
					# ,'null' if Row.get('OrderCreateDate') == None else '\'{0}\''.format(Row.get('OrderCreateDate'))
					# ,'null' if Row.get('SalesKind') == None else '\'{0}\''.format(Row.get('SalesKind'))
				# ]
				# bulk.find({"OrderID":OrderID,"GoodsID":GoodsID}).upsert().replace_one(Row)
			# oneRowSql = '('+(','.join(oneSqlRow))+')'
			# SqlRows.append(oneRowSql)
		# TruncateSql = "truncate table flnet.OrderRecord_Today;"
		# InsertSql = textwrap.dedent("""
# insert into flnet.OrderRecord_Today(PromoterID,InOutWarehouseDate,SellOrReturn,OrderID,GoodsID,GoodsFeeCode,GoodsManager,GoodsManagerBG,NotTaxedGoodsPrice,NotTaxedGoodsAvgPrice,SalesCount,ReturnCount,OrderSalesAmount,OrderReturnAmount,SalesIncome,GoodsAvgCost,GoodsSumCost,GoodsGrossAmount,DiscountSumAmount,GoodsDiscount,ScoreDiscount,CouponDiscount,ExpessIncome,GiftCardAmount,OrderFrom,DCName,SalesCorporation,InOutWarehouseID,WarehouseCode,OrderFromProvince,OrderFromCity,ReceiveProvince,ReceiveCity,OrderLiability,SalesLiability,OrderFromIP,CompanyStoreID,OrderCreateDate,SalesKind) values 
# {0};
		# """).format(','.join(SqlRows))
		# AfterSql = "call flnet.SP_AfterInsertOrderRecord();"
		# try :
			# bulk.execute()
		# except Exception as inst:
			# print(inst)
		# try :
			
			# cursor = mysql.connection.cursor()
			# cursor.execute(TruncateSql)
			# cursor.execute(InsertSql)
			# cursor.execute(AfterSql)
			# mysql.connection.commit()
			# cursor.close()
			# return True
		# except Exception as inst:
			# logging.getLogger('error_Logger').error(inst)
			# logging.getLogger('error_Logger').handlers[0].flush()
			# with open('Output.sql','w') as ErrOut :
				# ErrOut.write(TruncateSql)
				# ErrOut.write('\n')
				# ErrOut.write(InsertSql)
				# ErrOut.write('\n')
				# ErrOut.write(AfterSql)
				# ErrOut.close()
		# return False
			
		
@restapi.resource('/Flnet/Order2')
class PostOrder2(Resource):
	
	def post(self):
		result = {'result':'Fail'}
		ContentEncoding = request.headers.get('Content-Encoding')
#		print (ContentEncoding)
#		logging.info('ContentEncoding:{0}'.format(ContentEncoding))
		if ContentEncoding == 'gzip' : 
			JsonData = [] 
			try :
				gzipdata = request.data
				data = gzip.decompress(gzipdata)
				JsonStr = str(data,encoding='utf8')
				JsonData = json.loads(JsonStr)
#				print(JsonData)
				self.WriteToKafka(gzipdata)
			except Exception as inst:
				print(inst)
			if JsonData.get('Password') is None or JsonData['Password'] != 'ABC' :
				return result
#			print(JsonData.get('OrderRecords'))
#			self.WriteToDb(JsonData.get('OrderRecords'))
			
			
			
		return result
	def WriteToDb(self,Rows) :
		print(Rows)
		return
	def WriteToKafka(self,gzipdata) :
#		print(current_app.config['Kafka_HOST'])
		producer = KafkaProducer(bootstrap_servers=current_app.config['Kafka_HOST']
			,retries=current_app.config['Kafka_TryCount'])
		producer.send(topic='topic3',value=gzipdata).get(timeout=30)
		producer.close(timeout=60)
		return

# @restapi.resource('/Flnet/UploadWeekReport')
# class UploadFlnetWeekReport(Resource):
	# def post(self):
		# result = {'result':'Fail'}
		# if 'file' not in request.files:
			# return result
		# file = request.files['file']
		# filename = file.filename
		# file.save(os.path.join(current_app.config['Flnet_WeekReportDir'], filename))
		
		
		# return result

