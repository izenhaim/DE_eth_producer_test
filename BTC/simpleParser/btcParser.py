import datetime

import requests
import json
from time import sleep
from kafka import KafkaProducer

from logService.logService import LogService

#

if __name__ == "__main__":

	logger = LogService(is_logging_on=True, log_output=LogService.file_output, file_path="./btcParserLog")

	# kafka_ip = "localhost"
	kafka_ip = "192.168.53.11"
	kafka_port = "9092"

	logger.log("starting with kafka config: " + kafka_ip + ":" + str(kafka_port))

	kafka_store_topic = 'btc-blocks'
	kafka_cleaned_topic = 'stats'

	kafka_sender = KafkaProducer(bootstrap_servers=kafka_ip + ":" + str(kafka_port))

	last_stored_height = 0

	while True:
		btc_main_status = json.loads(
			requests.get('https://api.blockcypher.com/v1/btc/main').content.decode('utf-8'))

		latest_height = int(btc_main_status["height"])

		while latest_height > last_stored_height:
			latest_block = json.loads(requests.get('https://api.blockcypher.com/v1/btc/main/blocks/' +
												   str(latest_height)).content.decode('utf-8'))

			required_data = {}
			required_data["height"] = latest_block["height"]
			required_data["total"] = latest_block["total"]
			required_data["fees"] = latest_block["fees"]
			required_data["n_tx"] = latest_block["n_tx"]
			required_data["time"] = int(
				datetime.datetime.strptime(latest_block["time"], "%Y-%m-%dT%H:%M:%SZ").timestamp())

			kafka_sender.send(kafka_store_topic, json.dumps(required_data).encode('utf-8'))
			kafka_sender.flush()

			logger.log("put block " + str(latest_block["height"]) + " in kafka storage topic")

			item = {"name": "btc.total", "value": latest_block["total"]}
			kafka_sender.send(kafka_cleaned_topic, json.dumps(item).encode('utf-8'))

			item = {"name": "btc.fees", "value": latest_block["fees"]}
			kafka_sender.send(kafka_cleaned_topic, json.dumps(item).encode('utf-8'))

			item = {"name": "btc.n_tx", "value": latest_block["n_tx"]}
			kafka_sender.send(kafka_cleaned_topic, json.dumps(item).encode('utf-8'))

			kafka_sender.flush()

			logger.log("put block " + str(latest_block["height"]) + " in kafka stats topic")

			last_stored_height = latest_height

		sleep(500)
