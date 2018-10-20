#!/usr/bin/python
# -*- coding:utf-8 -*-
import os
import sys
sys.path.insert(0, os.path.join('/Library', '/Frameworks/Python.framework/Versions/3.6/lib/python3.6/site-packages'))
from flask_script import Manager, Shell, Server, Option, Command
from app import create_app
from gunicorn import __version__
#import gunicorn
#from gunicorn.app.base import Application
#from gunicorn import Gunicorn
#import gunicorn.app.base
#from app.extensions import db
app = create_app(os.getenv('FLASK_CONFIG') or 'default')

manager = Manager(app)

def gunicornserver(host, port, workers):
	"""Start the Server with Gunicorn"""
	from gunicorn.app.base import Application
	
	class FlaskApplication(Application):
		def init(self, parser, opts, args):
			return {
				'bind': '{0}:{1}'.format(host, port),
				'workers': workers
			}
		def load(self):
			return app
	application = FlaskApplication()
	return application.run()

#access python shell with context
manager.add_command("shell",Shell(make_context=lambda: {'app': app, 'db': db}), use_ipython=True)
# run the app
manager.add_command("startserver",Server(port=(os.getenv('FLASK_PORT') or 5002), host='0.0.0.0',threaded=True))
###Windows Run 拿掉###
#manager.add_command("rungunicorn",gunicornserver(host = '0.0.0.0',port=5002,workers=4))
######
if __name__ == '__main__':
	manager.run()
