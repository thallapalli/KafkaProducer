package com.kt.learnkafka.kafkaproducer.producer;

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

	private void handleSucess(Integer key, String value, SendResult<Integer, String> result) {
		System.out.println("Message Sent Sucessfully -------->" + key + " and value is " + value + "  partition "
				+ result.getRecordMetadata().partition());
	}

	private void handleFailure(Integer key, String value, Throwable ex) {
		// TODO Auto-generated method stub

		System.out.println(
				"Message Not Sent  -------->" + key + " and value is " + value + " expetion " + ex.getMessage());
		try {
			throw ex;
		} catch (Throwable throwable) {
			System.out.println("Message Not Sent  -------->" + key + " and value is " + value + " expetion "
					+ throwable.getMessage());

		}

	}
}
