import bson
from flask import request, current_app
import time
import logging
import logging.config
import logging.handlers
import os

def configure_logger(app):
	#logging.config.fileConfig("./logger.conf")
	# 获取当前所有的logger
	#print(logging.Logger.manager.loggerDict.keys())
	#app.logger.info("logger start")
	init_log('./Log/Error/Error',logging.getLogger("error_Logger"),level=logging.ERROR)
	init_log('./Log/RequestLog/RequestLog',logging.getLogger("Request_Logger"),level=logging.INFO)
	init_log('./Log/Log',logging.getLogger("root"))
#	logging.getLogger('werkzeug').setLevel(logging.ERROR)
#	print(logging.Logger.manager.loggerDict.keys())

def init_log(log_path, logger=None, level=logging.INFO, when="D", backup=7,\
				format="%(levelname)s: %(asctime)s: %(filename)s:%(lineno)d * %(thread)d %(message)s",\
				datefmt="%m-%d %H:%M:%S"):
	"""
	init_log - initialize log module
	Args:
		log_path      - Log file path prefix.
		Log data will go to two files: log_path.log and log_path.log.wf
		Any non-exist parent directories will be created automatically
		logger        - default using logging.getLogger()
		level         - msg above the level will be displayed
						DEBUG < INFO < WARNING < ERROR < CRITICAL
						the default value is logging.INFO
		when          - how to split the log file by time interval
						'S' : Seconds
						'M' : Minutes
						'H' : Hours
						'D' : Days
						'W' : Week day
						default value: 'D'
		format        - format of the log
						default format:
						%(levelname)s: %(asctime)s: %(filename)s:%(lineno)d * %(thread)d %(message)s
						INFO: 12-09 18:02:42: log.py:40 * 139814749787872 HELLO WORLD
		backup        - how many backup file to keep
						default value: 7
		Raises:
						OSError: fail to create log directories
						IOError: fail to open log file
	"""
	formatter = logging.Formatter(format, datefmt)
	if not logger:
		logger = logging.getLogger()
	logger.setLevel(level)
	
	dir = os.path.dirname(log_path)
	if not os.path.isdir(dir):
		os.makedirs(dir)
	handler = logging.handlers.TimedRotatingFileHandler(log_path + ".log",when=when,backupCount=backup)
	handler.setLevel(level)
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	handler = logging.handlers.TimedRotatingFileHandler(log_path + ".log.wf",when=when,backupCount=backup)
	handler.setLevel(logging.WARNING)
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	
	
def logging_handler(app):

	"""
	Register 0 or more logger handles (mutates the app passed in).

	:param app: Flask application instance
	:return: None
	"""
	g = {}
	
	@app.before_request
	def before_request():
		"""
		Save time when the request started.

		:return: None
		"""
		g["start"] = time.time()
#		print('Before Request')
		return None

	@app.after_request
	def after_request(response):
		"""
		Write out a log entry for the request.

		:return: Flask response
		"""
		if 'start' in g:
			response_time = (time.time() - g["start"])
		else:
			response_time = 0

		response_time_in_ms = int(response_time * 1000)

		params = {
			'method': request.method,
			'in': response_time_in_ms,
			'url': request.path,
			'ip': request.remote_addr,
			'status_code':response.status_code
		}
		logging.getLogger('Request_Logger').info('%(method)s "%(url)s" Status Code[%(status_code)s] in %(in)sms for %(ip)s', params)
		logging.getLogger('Request_Logger').handlers[0].flush()
#		app.logger.info('%(method)s "%(url)s" in %(in)sms for %(ip)s', params)
#		logger.info('%(method)s "%(url)s" in %(in)sms for %(ip)s', params)
#		print('After Request')
		return response
		
	return None
