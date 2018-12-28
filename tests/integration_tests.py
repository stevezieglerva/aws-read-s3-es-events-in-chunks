import unittest
import time
import boto3
from lambda_function import *
import json
from S3TextFromLambdaEvent import *

event_two_files = {
  "Records": [
    {
      "eventID": "1",
      "eventVersion": "1.0",
      "dynamodb": {
        "Keys": {
          "s3-url": {
            "S": "https://s3.amazonaws.com/code-index/prep-output/dynamodb_stream_1.txt"
          }
        },
        "NewImage": {
          "Message": {
            "S": "New item!"
          },
          "s3-url": {
            "S": "http:"
          }
        },
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "SequenceNumber": "111",
        "SizeBytes": 26
      },
      "awsRegion": "us-east-1",
      "eventName": "INSERT",
      "eventSourceARN": "arn:aws:dynamodb:us-east-1:account-id:table/ExampleTableWithStream/stream/2015-06-27T00:48:05.899",
      "eventSource": "aws:dynamodb"
    },
    {
      "eventID": "1",
      "eventVersion": "1.0",
      "dynamodb": {
        "Keys": {
          "s3-url": {
            "S": "https://s3.amazonaws.com/code-index/prep-output/dynamodb_stream_2.css"
          }
        },
        "NewImage": {
          "Message": {
            "S": "New item!"
          },
          "s3-url": {
            "S": "http:"
          }
        },
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "SequenceNumber": "111",
        "SizeBytes": 26
      },
      "awsRegion": "us-east-1",
      "eventName": "INSERT",
      "eventSourceARN": "arn:aws:dynamodb:us-east-1:account-id:table/ExampleTableWithStream/stream/2015-06-27T00:48:05.899",
      "eventSource": "aws:dynamodb"
    }
    
  ]
}

class TestMethods(unittest.TestCase):

	def test_lambda_function__two_file_event__successful_results(self):
		# Arrange
		s3 = boto3.resource('s3')
		bucket = "code-index"
		key = "prep-input/dynamodb_stream_1.txt"
		file_text = "import java;\nprint('Hello world'); \n if x <> 5\n-;-"
		create_s3_text_file(bucket, key, file_text, s3)

		bucket = "code-index"
		key = "prep-input/dynamodb_stream_2.css"
		file_text = "html {color: red}"
		create_s3_text_file(bucket, key, file_text, s3)

		# Act
		result = lambda_handler(event_two_files, None)
		print(json.dumps(result, indent=3))

		# Assert
		self.assertEqual(result["msg"], "Success")
		self.assertEqual(result["input_file_count"], 2)


if __name__ == '__main__':
	unittest.main()		


