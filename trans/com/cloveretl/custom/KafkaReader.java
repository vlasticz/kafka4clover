package com.cloveretl.custom;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.jetel.component.AbstractGenericTransform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;

/**
 * KafkaReader
 * 
 * @author cholastal
 *
 */
public class KafkaReader extends AbstractGenericTransform {
	
	private final static String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	private final static String TOPIC = "test";
	
	private KafkaConsumer<String, String> consumer;
	
	@Override
	public void execute() {
						
		consumer.subscribe(Arrays.asList(TOPIC));
		consumer.seek(new TopicPartition(TOPIC, 0), 0L);
						
		DataRecord recordClo = outRecords[0];
							
		ConsumerRecords<String, String> records = consumer.poll(100);
				
		for(ConsumerRecord<String, String> recordKaf : records) {
			if(!getComponent().runIt()) {
				break;
			}
			
			recordClo.getField("offset").setValue(recordKaf.offset());
			recordClo.getField("content").setValue(recordKaf.value());
			writeRecordToPort(0, recordClo);		
		}
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		/** This way you can check connected edges and their metadata. */
		/*
		if (getComponent().getOutPorts().size() < 1) {
			status.add("Output port must be connected!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}

		DataRecordMetadata outMetadata = getComponent().getOutputPort(0).getMetadata();
		if (outMetadata == null) {
			status.add("Metadata on output port not specified!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}

		if (outMetadata.getFieldPosition("myMetadataFieldName") == -1) {
			status.add("Incompatible output metadata!", Severity.ERROR, getComponent(), Priority.NORMAL);
		}
		*/
		return status;
	}

	@Override
	public void init() {
		super.init();
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		
		try {
			Thread.currentThread().setContextClassLoader(null);
					
			Properties props = new Properties();
					
			props.put("bootstrap.servers", "localhost:9092");
		    props.put("group.id", "test");
		    props.put("enable.auto.commit", "true");
		    props.put("auto.commit.interval.ms", "1000");
		    props.put("session.timeout.ms", "30000");		    
		    props.put("key.deserializer", STRING_DESERIALIZER);
		    props.put("value.deserializer", STRING_DESERIALIZER);
		    
		    consumer = new KafkaConsumer<>(props);
		    
		} finally {
			Thread.currentThread().setContextClassLoader(cl);
		}
	    
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
	}
}
