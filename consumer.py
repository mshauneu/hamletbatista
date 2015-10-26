from kafka import KafkaConsumer

consumer = KafkaConsumer('title', group_id='title', bootstrap_servers=['localhost:9092'])

for message in consumer:
    print (message.value)
