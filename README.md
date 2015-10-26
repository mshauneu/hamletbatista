# Prepare 

Assuming `Java 8`, `Scala 2.11`, `maven`, `python3 and pip3` are installed.

Clone project:

`git clone https://github.com/mshauneu/hb.git && cd hb`

Download Kafka: 

`wget https://bintray.com/artifact/download/vertx/downloads/vert.x-3.1.0-full.zip && unzip vert.x-3.1.0-full.zip && rm vert.x-3.1.0-full.zip`

Download Vert.x:

`wget http://ftp.byfly.by/pub/apache.org/kafka/0.8.2.2/kafka_2.11-0.8.2.2.tgz && tar -zxf kafka_2.11-0.8.2.2.tgz && rm kafka_2.11-0.8.2.2.tgz`

Run Kafka:

`kafka_2.11-0.8.2.2/bin/zookeeper-server-start.sh kafka_2.11-0.8.2.2/config/zookeeper.properties`
`kafka_2.11-0.8.2.2/bin/kafka-server-start.sh kafka_2.11-0.8.2.2/config/server.properties`

# Build 

`mvn package`

# Run Vert.x Verticles

## Producer
`export CLASSPATH="html-consumer/target/html-consumer-0.1.0.jar:html-consumer/target/lib/*";vert.x-3.1.0/bin/vertx run -cluster com.hb.HtmlConsumerVerticle`

## Consumer
`export CLASSPATH="html-parser/target/html-parser-0.1.0.jar:html-parser/target/lib/*";vert.x-3.1.0/bin/vertx run -cluster com.hb.HtmlParserVerticle -Ddb.path=./db`

# Test

## Install kafka lib:

`pip3 install kafka-python`

## Run consumer:

`python3 consumer.py`

## Run producer:

`python3 producer.py`

You should see tree titles from test/*.html

If you use tmux you can orginize panels like this:
![alt tag](https://github.com/mshauneu/hb/blob/master/screen.png)


# Contribute
Run `mvn eclipse:eclipse` and import projects to Eclipse IDE


