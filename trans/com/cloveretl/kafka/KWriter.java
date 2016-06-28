package com.cloveretl.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KWriter {
	
	Producer<String, String> producer;
	String msg;

	public static void main(String[] args) {
		KWriter kw = new KWriter("writing from KWriter");
		kw.init();
		kw.exec();

	}
	
	public KWriter(String msg) {
		this.msg = msg;
	}
	
	private void init() {
		
		Properties props = new Properties();
		
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<>(props);
	}
	
	private void exec() {
		
		producer.send(new ProducerRecord<String, String>("test", msg));
		producer.close();
		
	}

}
