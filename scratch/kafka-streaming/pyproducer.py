# Example for sending very simple messages to the "test" topic.
from kafka import KafkaProducer
from time import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
t0 = time()

for i in range(10):
	m = 'message %d' % (t0 + i) 
	producer.send('test', m.encode())
	producer.flush()
