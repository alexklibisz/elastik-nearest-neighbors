from kafka import KafkaConsumer

consumer = KafkaConsumer('test')
for msg in consumer:
	print (msg)
