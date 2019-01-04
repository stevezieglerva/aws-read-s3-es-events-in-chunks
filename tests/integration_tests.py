import unittest
import time
import boto3
from lambda_function import *
import json
from S3TextFromLambdaEvent import *
import json
import os



event_one_file = {
	"Records": [
		{
		"eventVersion": "2.0",
		"eventTime": "1970-01-01T00:00:00.000Z",
		"requestParameters": {
			"sourceIPAddress": "127.0.0.1"
		},
		"s3": {
			"configurationId": "testConfigRule",
			"object": {
			"eTag": "0123456789abcdef0123456789abcdef",
			"sequencer": "0A1B2C3D4E5F678901",
			"key": "prep-input/ProjectX/es_bulk_integration_test.txt",
			"size": 1024
			},
			"bucket": {
			"arn": "arn:aws:s3:::code-index",
			"name": "sourcebucket",
			"ownerIdentity": {
				"principalId": "EXAMPLE"
			}
			},
			"s3SchemaVersion": "1.0"
		},
		"responseElements": {
			"x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
			"x-amz-request-id": "EXAMPLE123456789"
		},
		"awsRegion": "us-east-1",
		"eventName": "ObjectCreated:Put",
		"userIdentity": {
			"principalId": "EXAMPLE"
		},
		"eventSource": "aws:s3"
		}
	]
	}

class TestMethods(unittest.TestCase):

	def test_lambda_handler__no_inputs__success(self):
		## Arrange
		s3 = boto3.resource("s3")
		file_contents = "{\"index\": {\"_index\": \"code-index\", \"_type\": \"doc\"}}\n{\"@timestamp_local\": \"2019-01-04T10:35:31.457401\", \"file_text\": \"import java; -n print('Hello world'); -n if x  5-n-;-\", \"filename\": \"https://s3.amazonaws.com/code-index/prep-output/ProjectX/integration_test_2.txt\", \"@timestamp\": \"2019-01-04T15:35:31.457401\"}\n"
		create_s3_text_file("code-index", "prep-input/ProjectX/es_bulk_integration_test.txt", file_contents, s3)

		# Act
		result = lambda_handler(event_one_file, None)

		# Arrange
		self.assertEqual(result["msg"], "Success")



##	def test_delete_file_urls_from_bucket__valid_file_urls__file_deleted(self):
##		# Arrange
##		bucket = "code-index"
##		directory = "es-bulk-files-input"
##
##		s3 = boto3.resource("s3")
##		create_s3_text_file("code-index", "test_integration_1.txt", "file contents 1", s3)
##		create_s3_text_file("code-index", "test_integration_2.txt", "file contents 2", s3)
##		file_url_array = ["http://s3.amazonaws.com/code-index/test_delete_integration_1.txt", "http://s3.amazonaws.com/code-index/test_delete_integration_2.txt"]
##
##		# Act
##		result = delete_file_urls(file_url_array, s3)
##
##		# Arrange
##		self.assertEqual(result, 2)
##
##
##	def test_delete_file_urls_from_bucket__missing_file_url__file_deleted(self):
##		# Arrange
##		bucket = "code-index"
##
##		s3 = boto3.resource("s3")
##		create_s3_text_file("code-index", "test_delete_integration_1.txt", "file contents 1", s3)
##		file_url_array = ["http://s3.amazonaws.com/code-index/test_delete_integration_1.txt", "http://s3.amazonaws.com/code-index/missing_file.txt"]
##
##		# Act
##		result = delete_file_urls(file_url_array, s3)
##
##		# Arrange
##		# invalid file urls will not cause any exceptions
##		self.assertEqual(result, 2)




if __name__ == '__main__':
	unittest.main()		


