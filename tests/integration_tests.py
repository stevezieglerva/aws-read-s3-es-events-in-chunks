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

	def test_get_files_from_bucket_directory__bucket_has_files_10_chunk_size__file_text_returned(self):
		# Arrange
		bucket = "code-index"
		directory = "es-bulk-files-input"
		chunk_size = 10
		s3 = boto3.resource("s3")

		# Act
		result = get_files_from_bucket_directory(bucket, "es-bulk-files-input/", s3, 10)

		# Arrange
		self.assertEqual(len(result), 10)

	def test_get_files_text_from_bucket_directory__bucket_has_files_10_chunk_size__file_text_returned(self):
		# Arrange
		bucket = "code-index"
		directory = "es-bulk-files-input"
		chunk_size = 10
		s3 = boto3.resource("s3")

		# Act
		result = get_files_text_from_bucket_directory(bucket, "es-bulk-files-input/", s3, 10)

		# Arrange
		self.assertEqual(len(result), 10)

	def test_delete_file_urls_from_bucket__valid_file_urls__file_deleted(self):
		# Arrange
		bucket = "code-index"
		directory = "es-bulk-files-input"

		s3 = boto3.resource("s3")
		create_s3_text_file("code-index", "test_integration_1.txt", "file contents 1", s3)
		create_s3_text_file("code-index", "test_integration_2.txt", "file contents 2", s3)
		file_url_array = ["http://s3.amazonaws.com/code-index/test_delete_integration_1.txt", "http://s3.amazonaws.com/code-index/test_delete_integration_2.txt"]

		# Act
		result = delete_file_urls(file_url_array, s3)

		# Arrange
		self.assertEqual(result, 2)


	def test_delete_file_urls_from_bucket__missing_file_url__file_deleted(self):
		# Arrange
		bucket = "code-index"

		s3 = boto3.resource("s3")
		create_s3_text_file("code-index", "test_delete_integration_1.txt", "file contents 1", s3)
		file_url_array = ["http://s3.amazonaws.com/code-index/test_delete_integration_1.txt", "http://s3.amazonaws.com/code-index/missing_file.txt"]

		# Act
		result = delete_file_urls(file_url_array, s3)

		# Arrange
		# invalid file urls will not cause any exceptions
		self.assertEqual(result, 2)


if __name__ == '__main__':
	unittest.main()		


