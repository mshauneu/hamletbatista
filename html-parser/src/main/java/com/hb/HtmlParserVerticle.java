package com.hb;

import static org.fusesource.lmdbjni.Constants.bytes;

import java.io.ByteArrayInputStream;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.ccil.cowan.tagsoup.jaxp.SAXParserImpl;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
/**
 * 
 * @author mike
 */
public class HtmlParserVerticle extends AbstractVerticle {

	private static final Logger logger = LoggerFactory.getLogger(HtmlParserVerticle.class);

	private static final String KAFKA_SERVERS = System.getProperty("kafkaServers", "127.0.0.1:9092");
	private static final String KAFKA_TOPIC = System.getProperty("kafkaTopic", "title"); 
	private static final String KAFKA_MESSAGE_KEY = System.getProperty("kafkaMessageKey", "title");
	
	private static final String BUS = System.getProperty("htmlBus", "html"); 

	
	private KafkaProducer<String, String> kafkaProducer;
	private Database db;
	private Env env;	
	
	
	private class TitleHandler extends DefaultHandler {
		
		private boolean readTitle = false;
		private StringBuilder title = new StringBuilder();

		public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {
			if (localName.equalsIgnoreCase("title")) {
				readTitle = true;
			}
		}

		public void endElement(String uri, String localName, String name) throws SAXException {
			if (localName.equalsIgnoreCase("title")) {
				readTitle = false;
			}
		}

		public void characters(char[] ch, int start, int length) throws SAXException {
			if (readTitle) {
				title.append(ch, start, length);
			}
		}
		
		public String getTitle() {
			return title.toString();
		}
	}	
	
	@Override
	public void start() throws Exception {
		
		EventBus eventBus = vertx.eventBus();
		
		kafkaProducer = createKafkaProducer();
		env = new Env(System.getProperty("db.path"));
		db = env.openDatabase();
		
		MessageConsumer<byte[]> consumer = eventBus.consumer(BUS);

		consumer.handler(message -> {
			TitleHandler handler = new TitleHandler();
			try {
				SAXParserImpl parser = SAXParserImpl.newInstance(null);
				parser.parse(new ByteArrayInputStream(message.body()), handler);
			} catch (Exception e) {
				logger.error("Error parsing file", e);
			}
			
			String title = handler.getTitle();
			
			db.put(bytes(new Timestamp(System.currentTimeMillis()).toString()), bytes(title));
			
            ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, KAFKA_MESSAGE_KEY, title);
            kafkaProducer.send(record, (metadata, exception) -> {
				if (exception == null) {
					message.reply("OK");
					logger.info("TITLE sent");
				} else {
					logger.error(exception);
				}
			});
		});		

	}

	@Override
	public void stop() throws Exception {
		if (kafkaProducer != null) {
            kafkaProducer.close();
        }		
		if (kafkaProducer != null) {
			kafkaProducer.close();
		}
		if (db != null) {
			db.close();
		}
		if (env != null) {
			env.close();
		}
	}
	
	private KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();


        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }
	
}