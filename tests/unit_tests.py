import unittest
import time
import boto3
from lambda_function import *
import json
from S3TextFromLambdaEvent import *

class TestMethods(unittest.TestCase):

	def test_create_csv_file_contents__simple_event__right_lines_and_data(self):
		# Arrange
		input = { "sample.txt" : "Here are the file contents.", 
			"second_file" : "ABC123"}

		# Act
		result = create_csv_file_contents(input)
		print(result)

		# Assert
		self.assertEqual(result.count("\n"), 2)
		self.assertEqual(result.count(","), 3)
		self.assertTrue("filename,file_text" in result)
		self.assertTrue("Here are the file contents." in result)

if __name__ == '__main__':
	unittest.main()		


