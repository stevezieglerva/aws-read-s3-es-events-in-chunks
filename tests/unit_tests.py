import unittest
import time
import boto3
from lambda_function import *
import json


class TestMethods(unittest.TestCase):
	def test_format_for_es_bulk__valid_input_files__format_is_correct(self):
		# Arrange
		input = {
				"log_event_unit_test.json" : "{ \"_index\": \"aws_lambda_logs\", \"_id\": \"\", \"data\": { \"lambda_name\": \"aws-s3-to-es\", \"file_refs\": { \"https://s3.amazonaws.com/aws-s3-to-es/general_events/general_2018-12-28-_04501.txt\": { \"bucket\": \"aws-s3-to-es\", \"key\": \"general_events/general_2018-12-28-_04501.txt\" } }, \"event\": \"got_file_refs\", \"logger\": \"lambda_function\", \"level\": \"critical\", \"timestamp\": \"2018-12-28T05:45:11.756545Z\", \"@timestamp\": \"2018-12-28T05:45:11.756\" } }"
				}
			

		# Act
		result = format_for_es_bulk(input)
		print("Result=" + result)

		# Assert
		self.assertTrue("{\"_index\":aws_lambda_logs\", \"_type\":\"doc\", \"_id\":\"\"}" in result)
		self.assertEqual(result.count("\n"), 2)


if __name__ == '__main__':
	unittest.main()		


