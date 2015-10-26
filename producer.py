from kafka import SimpleProducer, KafkaClient
import os

kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

for i in os.listdir('test'):
    if i.endswith(".html"):
        with open ('test/' + i, "rb") as htmlfile:
            data= htmlfile.read()
            producer.send_messages('html', data)
