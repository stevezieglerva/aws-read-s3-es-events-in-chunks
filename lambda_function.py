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
import uuid



def lambda_handler(event, context):
	try:
		aws_request_id = ""
		if context is not None:
			aws_request_id = context.aws_request_id

		shard = ""
		if "shard" in os.environ:
			shard = os.environ["shard"] + "/"

		start = datetime.datetime.now()
		print("Started " + str(start) + " " + shard + " " + aws_request_id)
		if "text_logging" in os.environ:
			log = structlog.get_logger()
		else:
			log = setup_logging("aws-read-s3-es-events-in-chunks", event, aws_request_id)

		s3 = boto3.resource("s3")
		if "chunk_size" not in os.environ:
			raise Exception("chunk_size environment variable not set")
		chunk_size = int(os.environ['chunk_size'])



		print("Getting files to check chunk size")
		file_text = get_files_text_from_bucket_directory("code-index", "es-bulk-files-input/" + shard, s3, chunk_size)
		print("Finished getting files")
		if len(file_text) == chunk_size:
#			log.critical("file_count_from_chunk", file_count=len(file_text), chunk_size=chunk_size)

			es_bulk_data = format_for_es_bulk(file_text)
			create_s3_text_file("code-index", "es-bulk-files-output/es_bulk_" + str(uuid.uuid4()) + " .json", es_bulk_data, s3)
			esl = ESLambdaLog()
			response = esl.load_bulk_data(es_bulk_data)

			bulk_load_http_status = {}
			bulk_load_http_status["100"] = 0
			bulk_load_http_status["200"] = 0
			bulk_load_http_status["300"] = 0
			bulk_load_http_status["400"] = 0
			bulk_load_http_status["500"] = 0
			for index_result in response["items"]:
				http_status = index_result["index"]["status"]
				http_group = str(http_status)[0] + "00"
				bulk_load_http_status[http_group] = bulk_load_http_status[http_group] + 1
#				if http_group != "200":
#					log.critical("bulk_index_item_failed", http_status_range=http_group, indexed_item=json.dumps(index_result))
#			for check in ["100", "200", "300", "400", "500"]:
#				log.critical("bulk_http_status", http_status_range=check, http_status_count=bulk_load_http_status[check])					
			print(bulk_load_http_status)
			file_urls = extract_s3_url_list_from_file_text_dict(file_text)
			delete_file_urls(file_urls, s3)
#			log.critical("process_results", file_count=len(file_text))
			end = datetime.datetime.now()		
			elapsed = end - start
			log.critical("processing_speed", chunk_size=chunk_size, successful_loaded_into_es=bulk_load_http_status["200"], elapsed_seconds=elapsed.seconds, docs_per_second=bulk_load_http_status["200"]/elapsed.seconds)
		else:
			print("Skpping since only " + str(len(file_text)) + " files available")
#			log.critical("skipping_not_enough", files_count_so_far=len(file_text))
		

		log.critical("finished")
		print("Finished " + str(start) + " " + shard + " " + aws_request_id)

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
	log = structlog.get_logger()

	bulk_format_template = "{{ \"index\" : {{ \"_index\" : \"{0}\", \"_type\" : \"doc\", \"_id\" : \"{1}\"}} }}\n{2}"
	bulk_data = ""
	for file in file_text.keys():
		if "_index" in file_text[file]:
			log_item = json.loads(file_text[file])
			index = log_item["_index"]
			id = log_item["_id"]
			data = log_item["data"]
			local_time = LocalTime()
			data["processed_for_bulk_utc"] = local_time.get_utc_timestamp()
			data["processed_for_bulk_local"] = local_time.get_local_timestamp()
			data_str = json.dumps(data)
			new_bulk_item = bulk_format_template.format(index, id, data_str)
			bulk_data = bulk_data + new_bulk_item + "\n"
			# log.critical("bulk_conversion", file=file)
		else:
			print("Skipping: " + file)
			# log.critical("bulk_conversion_skipped", file=file)
	return bulk_data