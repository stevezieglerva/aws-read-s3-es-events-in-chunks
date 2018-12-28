import unittest
import time
import boto3
from lambda_function import *
import json


class TestMethods(unittest.TestCase):

	def test_lambda_handler__no_inputs__success(self):
		# Arrange

		# Act
		result = lambda_handler("", None)

		# Arrange
		self.assertEqual(result["msg"], "Success")


if __name__ == '__main__':
	unittest.main()		


