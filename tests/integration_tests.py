import unittest
import time
import boto3
from lambda_function import *
import json
from S3TextFromLambdaEvent import *
import json


class TestMethods(unittest.TestCase):

	def test_lambda_handler__no_inputs__success(self):
		# Arrange

		# Act
		result = lambda_handler("", None)

		# Arrange
		self.assertEqual(result["msg"], "Success")

	def test_read_chunk_of_s3_files__bucket_has_files_10_chunk_size__file_text_returned(self):
		# Arrange
		bucket = "code-index"
		directory = "es-bulk-files-input"
		chunk_size = 10
		s3 = boto3.resource("s3")

		# Act
		result = get_files_from_bucket_directory(bucket, "es-bulk-files-input/", s3, 10)

		# Arrange
		self.assertEqual(len(result), 10)
    





if __name__ == '__main__':
	unittest.main()		


