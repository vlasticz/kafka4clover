package com.cloveretl.kafka;

import java.util.Properties;
// import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetel.component.AbstractGenericTransform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.string.StringUtils;

/**
 * Example component that serves as an Apache Kafka producer.
 * Uses Apache Kafka 0.8.2 API.
 * 
 * Supports appending messages to the specified topic.
 * One record is one message in kafka log.
 * 
 * @author salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 */
public class KafkaWriter extends AbstractGenericTransform {
	
	// custom attributes
	private final static String HOST_ATTRIBUTE = "host";
	private final static String PORT_ATTRIBUTE = "port";
	private final static String TOPIC_ATTRIBUTE = "topic";
	private static final String MAX_REJECTED_ATTR = "maxRejected";

	// expected fields in input/output metadata
	private final static String METADATA_CONTENT_FIELD = "content";
	
	private final static String DICTIONARY_FAIL_ENTRY_NAME = "fail";
	
	private final static String BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
	private final static long TIMEOUT = 5_000L; // ms
	
	private String topic;
	private KafkaProducer<String, byte[]> producer;
	private AtomicInteger waitingForCallback = new AtomicInteger(0);
	private boolean errorEdgeConnected = false;
	private long allowedRejectsLeft = 0;
	private long maxRejects = 0;
	private boolean maxRejectsExceeded = false;
	
	private class WriteCallback implements Callback {
		
		// record value, held in case we need to write it to error port
		byte[] content;
		
		public WriteCallback(byte[] content) {
			this.content = content;
		}

		/** This is executed by background IO thread of kafka producer - be careful with race conditions. */
		@Override
		public void onCompletion(RecordMetadata meta, Exception ex) {
			
			if (ex != null) {
				getLogger().debug("Record rejected, exception: " + ExceptionUtils.getMessage(ex));
				
				if (errorEdgeConnected) {
					outRecords[0].getField(METADATA_CONTENT_FIELD).setValue(content);
					writeRecordToPort(0, outRecords[0]);
				}
				
				if (allowedRejectsLeft == 0) {
					// we reached the point where we should fail the graph
					maxRejectsExceeded = true;
					try {
						getGraph().getDictionary().setValue(DICTIONARY_FAIL_ENTRY_NAME, true);
					} catch (ComponentNotReadyException e) {
						getLogger().error("Can't set dictionary entry after max rejects was exceeded.");
					}
				}
				allowedRejectsLeft--;
			}
			waitingForCallback.decrementAndGet();
		}
	}

	@Override
	public void execute() {
		try {
			// clover record used for reading component's input
			DataRecord record;

						
			while (getComponent().runIt() && (record = readRecordFromPort(0)) != null) {
				if (maxRejectsExceeded) {
					// stop sending records
					break;
				}
				
				ProducerRecord<String, byte[]> producerRecord;
				WriteCallback callback;
				if (errorEdgeConnected) {
					byte[] content = (byte[]) record.getField(METADATA_CONTENT_FIELD).getValueDuplicate();
					producerRecord = new ProducerRecord<String, byte[]>(topic, content);
					
					// record value held in callback - CAN BE MEMORY INTENSIVE WHEN CALLBACKS COME LATE
					callback = new WriteCallback(content);
				} else {
					byte[] content = (byte[]) record.getField(METADATA_CONTENT_FIELD).getValue();
					producerRecord = new ProducerRecord<String, byte[]>(topic, content);
					
					// no holding of values in memory when we don't need to
					callback = new WriteCallback(null);
				}

				// send kafka record
				// Future<RecordMetadata> f = producer.send(producerRecord, callback);
				producer.send(producerRecord, callback);

				waitingForCallback.incrementAndGet();

				// call get() to detect failure states (this can be used for synchronous writer)
				// f.get();
			}
			
			getLogger().debug("Waiting for callbacks");
			// wait for all callbacks
			while (getComponent().runIt() && waitingForCallback.get() > 0) {
				Thread.sleep(50);
			}
			if (waitingForCallback.get() == 0) {
				getLogger().debug("All callbacks came.");
			} else {
				getLogger().debug("Number of callbacks does not equal number of sent records.");
			}
			
		} catch (Exception e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public void init() {
		super.init();
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		return status;
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
				
		errorEdgeConnected = getGraph().getSubgraphOutputPorts().getPorts().get(0).isConnected();
		
		// read attributes
		String host = getProperties().getStringProperty(HOST_ATTRIBUTE);
		String port = getProperties().getStringProperty(PORT_ATTRIBUTE);
		topic = getProperties().getStringProperty(TOPIC_ATTRIBUTE);
		
		String maxRejectedResolved = getProperties().getStringProperty(MAX_REJECTED_ATTR);
		if(maxRejectedResolved.equals("MIN_INT")) {
			maxRejects = Integer.MIN_VALUE;
		} else if(maxRejectedResolved.equals("MAX_INT")) {
			maxRejects = Integer.MAX_VALUE;
		} else {
			maxRejects = getProperties().getLongProperty(MAX_REJECTED_ATTR, 0);
		}
		allowedRejectsLeft = maxRejects;
		
		// create producer config
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		
		try {
			Thread.currentThread().setContextClassLoader(null);
			
			Properties kafkaProducerConfig = new Properties();
			String brokerList;
			if (StringUtils.isEmpty(port)) {
				brokerList = host;
			} else {
				brokerList = host + ":" + port;
			}
			kafkaProducerConfig.put("bootstrap.servers", brokerList);
			kafkaProducerConfig.put("timeout.ms", Long.toString(TIMEOUT));
			kafkaProducerConfig.put("metadata.fetch.timeout.ms", Long.toString(TIMEOUT));
			kafkaProducerConfig.put("key.serializer", BYTE_ARRAY_SERIALIZER);
			kafkaProducerConfig.put("value.serializer", BYTE_ARRAY_SERIALIZER);
			
			producer = new KafkaProducer<String, byte[]>(kafkaProducerConfig);
		} finally {
			Thread.currentThread().setContextClassLoader(cl);
		}
	}		

	@Override
	public void postExecute() throws ComponentNotReadyException {
		if (producer != null) {
			producer.close();
		}
	}
}
