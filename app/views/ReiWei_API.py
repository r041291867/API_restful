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
            logging.getLogger('error_Logger').error('ConvenienceStore LH_type_contribution_day Query Err')
            logging.getLogger('error_Logger').error(inst)

        finally:
            cursor.close()
            conn.close()

        response = jsonify(result)
        response.status_code=200
        return result
