package com.cloveretl.custom;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetel.component.AbstractGenericTransform;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;

/**
 * This is an example custom writer. It shows how you can
 *  read records from input port and write to dictionary.
 */
public class KafkaWriter extends AbstractGenericTransform {
	
	Producer<String, String> producer;

	@Override
	public void execute() {
			
		producer.send(new ProducerRecord<String, String>("test", "Hello from CloverETL"));				
		producer.close();	
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		/** This way you can check connected edges and their metadata. */
		/*
		if (getComponent().getInPorts().size() < 1) {
			status.add("Input port must be connected!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}
		
		DataRecordMetadata inMetadata = getComponent().getInputPort(0).getMetadata();
		if (inMetadata == null) {
			status.add("Metadata on input port not specified!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}

		if (inMetadata.getFieldPosition("myMetadataFieldName") == -1) {
			status.add("Incompatible input metadata!", Severity.ERROR, getComponent(), Priority.NORMAL);
		}
		*/
		return status;
	}

	@Override
	public void init() {
		
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

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
	}
}
