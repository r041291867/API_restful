#!/usr/bin/python
# -*- coding:utf-8 -*-
import flask
from . import extensions, config
from . import views
from . import loggings
#from datetime import timedelta
#from flask import Flask, session, render_template

#from flask.ext.mongoengine import MongoEngineSessionInterface
#from flask_debugtoolbar import DebugToolbarExtension

def create_app(config_name='default'):
	"""Flask app factory

	:config_name: a string object.
	:returns: flask.Flask object

	"""

	app = flask.Flask(__name__)

	# set the config vars using the config name and current_app
	config.config[config_name](app)
	#print(app.config['MONGODB_HOST'])


	SetUrlMap(app)
	register_extensions(app)
	register_blueprints(app)
#	app.permanent_session_lifetime = timedelta(seconds=300)

#	DebugToolbarExtension(app)
	return app


def register_extensions(app):
	"""Call the method 'init_app' to register the extensions in the flask.Flask
	object passed as parameter.

	:app: flask.Flask object
	:returns: None

	"""

#	extensions.db.init_app(app)
	extensions.mysql.init_app(app)
	extensions.mysql2.init_app(app)
	# app.config['MYSQL_DATABASE_HOST']='192.168.2.51'
	# extensions.mysql51.init_app(app)
	extensions.restapi.init_app(app)
	extensions.cache.init_app(app, config={'CACHE_TYPE': 'simple'})
	extensions.mongo.init_app(app)
	extensions.compress.init_app(app)

#	app.logger.addHandler(handler)
	loggings.configure_logger(app)
	loggings.logging_handler(app)

def register_blueprints(app):
	"""Register all blueprints.

	:app: flask.Flask object
	:returns: None

	"""
	app.register_blueprint(views.views_blueprint)

def SetUrlMap(app):
	app.url_map.converters['StartMonthToEndMonth'] = utils.StartMonthToEndMonth
	app.url_map.converters['StartDateToEndDate'] = utils.StartDateToEndDate
	app.url_map.converters['YearMonthQuarterly'] = utils.YearMonthQuarterly
	app.url_map.converters['DateStartHourToEndHour'] = utils.DateStartHourToEndHour
