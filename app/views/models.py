#!/usr/bin/python
# -*- coding:utf-8 -*-
from app.extensions import db


class EMP_INFO_20161212(db.Document):

	"""User model """

	EMP_NAME = db.StringField()
	SEX = db.StringField()

	def to_json2(self):
		"""Returns a json representantion of the user.
		:returns: a json object.

		"""

		return {
			'EMP_NAME': str(self.EMP_NAME),
			'SEX': self.SEX
		}
