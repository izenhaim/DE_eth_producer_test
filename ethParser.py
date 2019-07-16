import requests
import json
from time import sleep
from kafka import KafkaProducer

from logService import LogService

if __name__ == "__main__":

		logger = LogService()
		logger.turn_logging_on()

		# kafka_ip = "localhost"
		kafka_ip = "192.168.53.11"
		kafka_port = "9092"

		logger.log("starting with kafka config: " + kafka_ip + ":" + str(kafka_port))

		kafka_topic = 'test'

		kafka_sender = KafkaProducer(bootstrap_servers=kafka_ip + ":" + str(kafka_port))

		last_stored_height = 0

		while True:
				eth_main_status = json.loads(requests.get('https://api.blockcypher.com/v1/eth/main').content.decode('utf-8'))

				latest_height = int(eth_main_status["height"])

				if latest_height > last_stored_height:
					latest_block = json.loads(requests.get('https://api.blockcypher.com/v1/eth/main/blocks/' +
																		 str(latest_height)).content.decode('utf-8'))

					required_data = {}
					required_data["height"] = latest_block["height"]
					required_data["total"] = latest_block["total"]
					required_data["fees"] = latest_block["fees"]
					required_data["n_tx"] = latest_block["n_tx"]

					kafka_sender.send(kafka_topic, json.dumps(required_data).encode('utf-8'))
					kafka_sender.flush()

					logger.log(json.dumps(required_data))
					last_stored_height = latest_height

				sleep(5)