import boto3
import time
import datetime
import logging
import structlog
import os
import json
import sys
from S3TextFromLambdaEvent import *
from ESLambdaLog import *



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

		if len(file_text) == chunk_size:
			log.critical("file_count_from_chunk", file_count=len(file_text), chunk_size=chunk_size)

			es_bulk_data = format_for_es_bulk(file_text)
			create_s3_text_file("code-index", "es-bulk-files-output/es_bulk.json", es_bulk_data, s3)
			print("\n\n\Bulk data string:" + es_bulk_data)
			esl = ESLambdaLog()
			response = esl.load_bulk_data(es_bulk_data)
			print("bulk_data_response: ")
			print(json.dumps(response, indent=3))
			#if response["errors"] == True:
			#	raise Exception("Bulk didn't load")

			file_urls = extract_s3_url_list_from_file_text_dict(file_text)
			delete_file_urls(file_urls, s3)
			log.critical("process_results", file_count=len(file_text))
		else:
			print("Skpping since only " + str(len(file_text)) + " files available")
		log.critical("finished")
		print("Finished")

	except Exception as e:
		log.exception()
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


def format_for_es_bulk(file_text):
	#{"index":{"_index":"brain3", "_type":"doc", "_id":"c--Users-18589-.sdfsd"}
	#{"brain_type" : "file", "title" : ".aws", "desc" : "c:\\Users\\pop\\.sdfds", "date" : "08/27/2018", "date_date-month" : "08", "date_date-day" : "27", "date_date-year" : "2018", "@timestamp":"2018-08-27T00:00:00", "bytes" : "<DIR>", "dir-eg" : "c:\\Users\\pop", "file" : ".aws", "ext" : "aws", "source" : "file-c:\\Users\\pop\\.sdfsds"}

	bulk_format_template = "{{ \"index\" : {{ \"_index\":\"{0}\", \"_type\":\"doc\"}} }}\n{2}"
	bulk_data = ""
	for file in file_text.keys():
		print("\nConverting bulk file: " + file)

		if "_index" in file_text[file]:
			log_item = json.loads(file_text[file])
			print("Log item: " + str(log_item))
			print(type(log_item))
			index = log_item["_index"]
			id = log_item["_id"]
			data = log_item["data"]
			print("data:")
			print(data)
			data_str = json.dumps(data)
			print("data_str:")
			print("\t" + index)
			new_bulk_item = bulk_format_template.format(index, id, data_str)
			bulk_data = bulk_data + new_bulk_item + "\n"
			print("\tAdded to bulk")
		else:
			print("Skipping: " + file)
	return bulk_data