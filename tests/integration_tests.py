import unittest
import time
import boto3
from lambda_function import *
import json
from S3TextFromLambdaEvent import *
import json
import os


class TestMethods(unittest.TestCase):

	def test_lambda_handler__no_inputs__success(self):
		# Arrange
		os.environ["chunk_size"] = "10"
		# Act
		result = lambda_handler("", None)

		# Arrange
		self.assertEqual(result["msg"], "Success")

	def test_get_files_from_bucket_directory__bucket_has_files_10_chunk_size__file_text_returned(self):
		# Arrange
		bucket = "code-index"
		directory = "es-bulk-files-input"
		chunk_size = 5
		s3 = boto3.resource("s3")
		create_s3_text_file("code-index", directory + "/test_integration_a_1.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)
		create_s3_text_file("code-index", directory + "/test_integration_a_2.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)
		create_s3_text_file("code-index", directory + "/test_integration_a_3.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)
		create_s3_text_file("code-index", directory + "/test_integration_a_4.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)
		create_s3_text_file("code-index", directory + "/test_integration_a_5.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)
		create_s3_text_file("code-index", directory + "/test_integration_a_6.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)

		# Act
		result = get_files_from_bucket_directory(bucket, "es-bulk-files-input/", s3, chunk_size)

		# Arrange
		self.assertEqual(len(result), chunk_size)

	def test_get_files_text_from_bucket_directory__bucket_has_files_10_chunk_size__file_text_returned(self):
		# Arrange
		bucket = "code-index"
		directory = "es-bulk-files-input"
		chunk_size = 5
		s3 = boto3.resource("s3")
		create_s3_text_file("code-index", directory + "/test_integration_a_1.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)
		create_s3_text_file("code-index", directory + "/test_integration_a_2.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)
		create_s3_text_file("code-index", directory + "/test_integration_a_3.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)
		create_s3_text_file("code-index", directory + "/test_integration_a_4.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)
		create_s3_text_file("code-index", directory + "/test_integration_a_5.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)
		create_s3_text_file("code-index", directory + "/test_integration_a_6.txt", "{\"_index\": \"code-index\", \"_id\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"data\": {\"filename\": \"https://s3.amazonaws.com/code-index/prep-output/8_2019-01-01-_91440.txt\", \"file_text\": \"Test 8 \\n\", \"@timestamp\": \"2019-01-01T14:16:14.983763\", \"@timestamp_local\": \"2019-01-01T09:16:14.983763\"}}", s3)

		# Act
		result = get_files_text_from_bucket_directory(bucket, "es-bulk-files-input/", s3, chunk_size)

		# Arrange
		self.assertEqual(len(result), chunk_size)

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



	def test_check_no_single_quotes(self):
		# Arrange
		s3 = boto3.resource("s3")
		create_s3_text_file("code-index", "integration_test_single_quotes.json", "{\"hello\" : \"world\"}", s3)
		file_url_array = ["http://s3.amazonaws.com/code-index/integration_test_single_quotes.json"]

		# Act
		file_text = get_file_text_from_s3_urls(file_url_array, s3)
		print("***file_text:")
		print(file_text)

		# Arrange
		result = file_text["http://s3.amazonaws.com/code-index/integration_test_single_quotes.json"]
		self.assertEqual(result, "{\"hello\" : \"world\"}")


if __name__ == '__main__':
	unittest.main()		


