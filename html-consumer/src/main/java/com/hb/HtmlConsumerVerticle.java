package com.hb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 
 * @author mike
 */
public class HtmlConsumerVerticle extends AbstractVerticle {

	private static final String ZOOKEEPER_CONNECT = System.getProperty("zookeeperConnect", "localhost:2181"); 
	private static final String KAFKA_TOPIC = System.getProperty("kafkaTopic", "html"); 

	private static final String BUS = System.getProperty("htmlBus", "html"); 
	
	private Logger logger = LoggerFactory.getLogger(HtmlConsumerVerticle.class);
	
	private ExecutorService executor;
	private ConsumerConnector consumer;
	
	public class ConsumerTask implements Runnable {
		
		private EventBus eventBus;
	    private KafkaStream<byte[], byte[]> stream;
	 
	    public ConsumerTask(EventBus eventBus, KafkaStream<byte[], byte[]> stream) {
	    	this.eventBus = eventBus;
	        this.stream = stream;
	    }
	 
	    public void run() {
	        for (ConsumerIterator<byte[], byte[]> it = stream.iterator(); it.hasNext(); ) {
	        	eventBus.send(BUS, it.next().message());
	        	logger.info("HTML sent");
	        }
	    }
	}	
	
	@Override
	public void start() throws Exception {
		EventBus eventBus = vertx.eventBus();

		new Thread(() -> {
			consumer = createKafkaConsumer();
			
			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			topicCountMap.put(KAFKA_TOPIC, new Integer(Runtime.getRuntime().availableProcessors()));
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(KAFKA_TOPIC);

			executor = Executors.newFixedThreadPool(streams.size());
			streams.forEach(stream -> {
				executor.submit(new ConsumerTask(eventBus, stream));
			}); 
			
		}).start();;
	}
	
	@Override
	public void stop() throws Exception {
		if (consumer != null) {
			 consumer.shutdown();
		}
		if (executor != null) {
			executor.shutdown();
	        try {
	            executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
	        } catch (InterruptedException e) {
	        }		
		}

	}
	
	private ConsumerConnector createKafkaConsumer() {
		Properties props = new Properties();
        props.put("group.id", "html");
        props.put("auto.commit.interval.ms", "1000");
        props.put("zookeeper.connect", ZOOKEEPER_CONNECT);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}
	
}