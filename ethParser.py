import requests
import json
import sys
from time import sleep
from kafka import KafkaProducer

if __name__ == "__main__":

	# kafka_ip = "localhost"
	kafka_ip = "192.168.53.11"
	kafka_port = "9092"
	# kafka_ip = sys.argv[1]
	# kafka_port = int(sys.argv[2])

	kafka_topic = "test_topic"

	kafka_sender = KafkaProducer(bootstap_server=[kafka_ip + ":" + str(kafka_port)],
								 value_serializer=lambda x: json.dumps(x).encode('utf-8'))

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

			kafka_sender.send(kafka_topic, value=json.dumps(required_data))

			last_stored_height = latest_height

			sleep(5)






