package com.kt.learnkafka.kafkaproducer.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kt.learnkafka.kafkaproducer.domian.Event;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	@Autowired
	ObjectMapper objectMapper;
	private static String topic="event";

	public void sendLibraryEvent(Event event) throws JsonProcessingException {
		Integer key = event.getEventId();
		String value = objectMapper.writeValueAsString(event);

		ListenableFuture<SendResult<Integer, String>> sendDefault = kafkaTemplate.sendDefault(key, value);
	
		sendDefault.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				// TODO Auto-generated method stub
				handleSucess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {
				// TODO Auto-generated method stub
				handleFailure(key, value, ex);

			}

		});

	}
	
	public void sendLibraryEvent_Approach2(Event event) throws JsonProcessingException {
		log.debug("Inside sendLibraryEvent_Approach2 ");
		Integer key = event.getEventId();
		String value = objectMapper.writeValueAsString(event);
		ProducerRecord<Integer, String> producerRecord =buildProducerRecord(key,value,topic);
		ListenableFuture<SendResult<Integer, String>> sendDefault = kafkaTemplate.send(producerRecord);
	
		sendDefault.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				// TODO Auto-generated method stub
				handleSucess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {
				// TODO Auto-generated method stub
				handleFailure(key, value, ex);

			}

		});
		log.debug("Done sendLibraryEvent_Approach2 ");
		

	}
	
	
	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic2) {
		// TODO Auto-generated method stub
		ProducerRecord<Integer, String>  producerRecord =null;
		List<Header> listOfHeaders=new ArrayList<Header>();
		RecordHeader recordHeader=new RecordHeader("event-source", "scanner".getBytes());
		RecordHeader recordHeader1=new RecordHeader("event-source1", "scanner1".getBytes());
		
		listOfHeaders.add(recordHeader);
		listOfHeaders.add(recordHeader1);
		producerRecord=new ProducerRecord<Integer, String>(topic2, null, key, value,listOfHeaders);
		
		return producerRecord;
	}

	

	public SendResult<Integer, String> sendLibraryEventSynchronous(Event event)
			throws JsonProcessingException, InterruptedException, ExecutionException {
		Integer key = event.getEventId();
		SendResult<Integer, String> sendResult=null;
		String value = objectMapper.writeValueAsString(event);
        try {
        	sendResult= kafkaTemplate.sendDefault(key, value).get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			log.error("InterruptedException  "+e.getMessage());
			throw e;
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			log.error("InterruptedException  "+e.getMessage());
			throw e;
		}
        return sendResult;
	}
	

	private void handleSucess(Integer key, String value, SendResult<Integer, String> result) {
		System.out.println("Message Sent Sucessfully -------->" + key + " and value is " + value + "  partition "
				+ result.getRecordMetadata().partition());
	}

	private void handleFailure(Integer key, String value, Throwable ex) {
		// TODO Auto-generated method stub

		System.out.println(
				"Message Not Sent--------------------------------------------------------------------------------------------------------------------  -------->" + key + " and value is " + value + " expetion " + ex.getMessage());
		try {
			throw ex;
		} catch (Throwable throwable) {
			System.out.println("Message Not Sent  -------->" + key + " and value is " + value + " expetion "
					+ throwable.getMessage());

		}

	}
}
