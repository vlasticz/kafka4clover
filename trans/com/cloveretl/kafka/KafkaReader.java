package com.cloveretl.kafka;

import java.io.UnsupportedEncodingException;
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
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * KafkaReader
 * 
 * Uses Kafka API 0.10.0.0
 * 
 * @author cholastal(lukas.cholasta@cloveretl.com)
 *
 */
public class KafkaReader extends AbstractGenericTransform {
		
	// Consumer attributes. Default values, might need to be changed.
	private final static String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
	private final static String SESSION_TIMEOUT = "30000";
	private final static String AUTO_COMMIT_INTERVAL = "1000";
	private final static int POLL_INTERVAL = 100;
	
	// Expected output metadata fields.
	private final static String OFFSET_METADATA_FIELD = "offset";
	private final static String CONTENT_METADATA_FIELD = "content";
	
	private boolean quit = false;
	
	private KafkaConsumer<String, byte[]> consumer;
	
	@Override
	public void execute() throws UnsupportedEncodingException {
		
		try{
			DataRecord cloverRecord = outRecords[0];
			ConsumerRecord<String, byte[]> lastRecord = null, 
					   					   currentRecord = null;
	
			long start = System.currentTimeMillis();
			
			while(!quit && getComponent().runIt()) {
	
				ConsumerRecords<String, byte[]> records = consumer.poll(POLL_INTERVAL);
	
				for(ConsumerRecord<String, byte[]> kafkaRecord : records) {
					cloverRecord.getField(OFFSET_METADATA_FIELD).setValue(String.valueOf(kafkaRecord.offset()));				
					cloverRecord.getField(CONTENT_METADATA_FIELD).setValue(new String(kafkaRecord.value(), getProperties().getStringProperty("logCharset")));
					writeRecordToPort(0, cloverRecord);
	
					currentRecord = kafkaRecord;
				}
	
				if(lastRecord != null) {
					// If the last record's offset is the same as the current one we're at the end of the stream - quit.
					if(lastRecord.offset() == currentRecord.offset())	            	
						quit = true;
				} else {
					// If is null and the read timeout is reached - quit.
					if(System.currentTimeMillis() - start > getProperties().getLongProperty("readTimeout"))
						quit = true;
				}
	
				lastRecord = currentRecord;			
			}			
			
		} catch(Exception e) {
			throw new JetelRuntimeException(e);
			
		} finally {
			consumer.close();
		}
	}
		
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		
		if (getComponent().getOutPorts().size() < 1) {
			status.add("Output port must be connected!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}

		DataRecordMetadata outMetadata = getComponent().getOutputPort(0).getMetadata();
		
		if (outMetadata.getFieldPosition(OFFSET_METADATA_FIELD) == -1) {
			status.add("Incompatible output metadata!", Severity.ERROR, getComponent(), Priority.NORMAL);
		}
		
		if (outMetadata.getFieldPosition(CONTENT_METADATA_FIELD) == -1) {
			status.add("Incompatible output metadata!", Severity.ERROR, getComponent(), Priority.NORMAL);
		}
		
		return status;
	}

	@Override
	public void init() {
		super.init();
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		
		// This classloader change is needed until the CloverETL 4.2.1 is out.
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		
		try {
			Thread.currentThread().setContextClassLoader(null);
			
			// Create consumer config.
			Integer partitionNum = getProperties().getIntProperty("partition");
			String topic = getProperties().getStringProperty("topic");
												
			Properties props = new Properties();
			
			props.put("bootstrap.servers", String.join(":", getProperties().getStringProperty("host"), getProperties().getStringProperty("port")));
			props.put("group.id", topic);
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", AUTO_COMMIT_INTERVAL);
			props.put("session.timeout.ms", SESSION_TIMEOUT);	
		    props.put("key.deserializer", STRING_DESERIALIZER);
		    props.put("value.deserializer", STRING_DESERIALIZER);
		    
		    consumer = new KafkaConsumer<>(props);
		    
		    // If a partition number is set, it is used. Otherwise it is assigned by the Kafka server. 
		    if(partitionNum != null) {
				TopicPartition partition = new TopicPartition(topic, partitionNum);
				consumer.assign(Arrays.asList(partition));
				consumer.poll(0);
				consumer.seek(partition, getProperties().getIntProperty("offset"));
				
			} else {			
				consumer.subscribe(Arrays.asList(topic));
				consumer.poll(0);		
				for (TopicPartition partition: consumer.assignment())
					consumer.seek(partition, getProperties().getIntProperty("offset"));
			}
		    
		} finally {
			Thread.currentThread().setContextClassLoader(cl);
		}	    
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();		
	}
}
