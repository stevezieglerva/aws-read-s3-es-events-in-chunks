import boto3
import time
import datetime
import logging
import structlog
import os
import json
import sys
from S3TextFromLambdaEvent import *



def lambda_handler(event, context):
	try:
		aws_request_id = ""
		if context is not None:
			aws_request_id = context.aws_request_id

		print("Started")
		if "text_logging" in os.environ:
			log = structlog.get_logger()
		else:
			log = setup_logging("aws-read-s3-es-events-in-chunks", event, aws_request_id)

		s3 = boto3.resource("s3")
		chunk_size = 100
		file_text = get_files_text_from_bucket_directory("code-index", "es-bulk-files-input/", s3, chunk_size)
		log.critical("file_count_from_chunk", file_count=len(file_text), chunk_size=chunk_size)


		file_urls = extract_s3_url_list_from_file_text_dict(file_text)
		delete_file_urls(file_urls, s3)
		log.critical("process_results", file_count=len(file_text))
		log.critical("finished")
		print("Finished")

	except Exception as e:
		#log.exception()
		print("Exception: "+ str(e))
		raise(e)
		return {"msg" : "Exception" }

	return {"msg" : "Success"}


def setup_logging(lambda_name, lambda_event, aws_request_id):
	logging.basicConfig(
		format="%(message)s",
		stream=sys.stdout,
		level=logging.INFO
	)
	structlog.configure(
		processors=[
			structlog.stdlib.filter_by_level,
			structlog.stdlib.add_logger_name,
			structlog.stdlib.add_log_level,
			structlog.stdlib.PositionalArgumentsFormatter(),
			structlog.processors.TimeStamper(fmt="iso"),
			structlog.processors.StackInfoRenderer(),
			structlog.processors.format_exc_info,
			structlog.processors.UnicodeDecoder(),
			structlog.processors.JSONRenderer()
		],
		context_class=dict,
		logger_factory=structlog.stdlib.LoggerFactory(),
		wrapper_class=structlog.stdlib.BoundLogger,
		cache_logger_on_first_use=True,
	)
	log = structlog.get_logger()
	log = log.bind(aws_request_id=aws_request_id)
	log = log.bind(lambda_name=lambda_name)
	log.critical("started", input_events=json.dumps(lambda_event, indent=3))

	return log

def extract_s3_url_list_from_file_text_dict(file_texts):
	return file_texts.keys()

