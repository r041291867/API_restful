import bson
from flask import request
import urllib
from werkzeug.routing import BaseConverter
from datetime import date,datetime as dt, timedelta
import re

def is_a_valid_object_id(object_id):
	"""Verify if the value is valid as an object id.
	:object_id: a string object
	:returns: True or False

	"""
	return bson.objectid.ObjectId.is_valid(object_id)

def cache_key():
	return request.url
	
class StartMonthToEndMonth(BaseConverter) :
	regex = r'[0-9]{4}-[0-9]{2}(?:-|to)[0-9]{4}-[0-9]{2}'
	def to_python(self, value):
		m = re.match(r'([0-9]{4}-[0-9]{2})(?:-|to)([0-9]{4}-[0-9]{2})',value)
		if m != None:
			return {'Start':m.group(1),'End':m.group(2)}
		else :
			return None
	def to_url(self, value):
		return str(value)

class StartDateToEndDate(BaseConverter) :
	regex = r'[0-9]{4}-[0-9]{2}-[0-9]{2}(?:-|to)[0-9]{4}-[0-9]{2}-[0-9]{2}'
	def to_python(self, value):
		m = re.match(r'([0-9]{4}-[0-9]{2}-[0-9]{2})(?:-|to)([0-9]{4}-[0-9]{2}-[0-9]{2})',value)
		if m != None:
			return {'Start':dt.strptime(m.group(1),'%Y-%m-%d'),'End':dt.strptime(m.group(2),'%Y-%m-%d')}
		else :
			return None
	def to_url(self, value):
		return str(value)

class YearMonthQuarterly(BaseConverter) :
	regex = r'[0-9]{4}(?:(?:-[0-9]{2})|(?:[Qq][1-4]))'
	def to_python(self, value):
		m1 = re.match(r'([0-9]{4})-([0-9]{2})',value)
		m2 = re.match(r'([0-9]{4})[Qq]([1-4])',value)
		if m1 != None:
			Start = dt.strptime(m1.group(0)+'-01','%Y-%m-%d')
			endyear = int(m1.group(1))
			endmonth = int(m1.group(2)) + 1
			if endmonth == 13 : 
				endyear = endyear +1
				endmonth = 1
			oneday = timedelta(days=-1)
			End = dt.strptime('{0}-{1}-01'.format(endyear,endmonth),'%Y-%m-%d')
			End = End + oneday
			return {'Start':Start,'End':End}
		elif m2 != None :
			Quarterly = m2.group(2)
			if Quarterly == '1' :
				return {'Start':dt.strptime(m2.group(1)+'-01-01','%Y-%m-%d'),'End':dt.strptime(m2.group(1)+'-03-31','%Y-%m-%d')}
			elif Quarterly == '2' :
				return {'Start':dt.strptime(m2.group(1)+'-04-01','%Y-%m-%d'),'End':dt.strptime(m2.group(1)+'-06-30','%Y-%m-%d')}
			elif Quarterly == '3' :
				return {'Start':dt.strptime(m2.group(1)+'-07-01','%Y-%m-%d'),'End':dt.strptime(m2.group(1)+'-09-30','%Y-%m-%d')}
			elif Quarterly == '4' :
				return {'Start':dt.strptime(m2.group(1)+'-10-01','%Y-%m-%d'),'End':dt.strptime(m2.group(1)+'-12-31','%Y-%m-%d')}
		else :
			return None
	def to_url(self, value):
		return str(value)
		
class DateStartHourToEndHour(BaseConverter) :
	regex = r'[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}'
	def to_python(self, value):
		m = re.match(r'([0-9]{4}-[0-9]{2}-[0-9]{2})-([0-9]{2})-([0-9]{2})',value)
		if m != None:
			return {'Date':dt.strptime(m.group(1),'%Y-%m-%d'),'StartHour':m.group(2),'EndHour':m.group(3)}
		else :
			return None
	def to_url(self, value):
		return str(value)