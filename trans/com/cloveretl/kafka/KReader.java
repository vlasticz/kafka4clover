package com.cloveretl.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Testing reference class.
 * 
 */

public class KReader {
		
	private KafkaConsumer<String, String> consumer;
	private boolean quit = false;
	
	public static void main(String[] args) {
		
		new KReader().init();		
	}
	
	private void init() {
		
		Properties props = new Properties();
		
		props.put("bootstrap.servers", "localhost:9092");
	    props.put("group.id", "test");
	    props.put("enable.auto.commit", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("session.timeout.ms", "30000");
	    props.put("auto.offset.reset", "earliest");
	    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    
	    consumer = new KafkaConsumer<>(props);
	    
	    exec();
	}
	
	
	private void exec() {
		ConsumerRecord<String, String> lastRecord = null, currentRecord = null;
		
		System.out.println("Consumer started");
			
		consumer.subscribe(Arrays.asList("test"));
		consumer.poll(0);
		
		for (TopicPartition partition: consumer.assignment())
		    consumer.seek(partition, 0L);
				
			
	    while (!quit) {
	        ConsumerRecords<String, String> records = consumer.poll(100);
	        
	        for (ConsumerRecord<String, String> record : records) {	        	
	            System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
	            	            	            
	            currentRecord = record;
	        }
	        
	        if(lastRecord != null) {
            	
	            if(lastRecord.offset() == currentRecord.offset()) {
	            	System.out.println("Quitting ...");
	            	consumer.close();
	            	quit = true;
	            }
            }
	        
	        lastRecord = currentRecord;
	    }
	}
}
