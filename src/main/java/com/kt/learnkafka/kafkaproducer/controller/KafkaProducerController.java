package com.kt.learnkafka.kafkaproducer.controller;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kt.learnkafka.kafkaproducer.domian.Book;
import com.kt.learnkafka.kafkaproducer.domian.Event;
import com.kt.learnkafka.kafkaproducer.domian.EventType;
import com.kt.learnkafka.kafkaproducer.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class KafkaProducerController {
	@Autowired
	LibraryEventProducer libraryEventProducer;
	
	@PostMapping("/v1/event")
	public ResponseEntity<Event> postEvent(@RequestBody Event event ) throws JsonProcessingException, InterruptedException, ExecutionException{
		//Invoke Kafka producer
		log.debug("before sendLibraryEvent ");
		//libraryEventProducer.sendLibraryEvent(event);
		//libraryEventProducer.sendLibraryEventSynchronous(event);
		event.setVentType(EventType.NEW);
		
		libraryEventProducer.sendLibraryEvent_Approach2(event);
		log.debug("after sendLibraryEvent ");
		return ResponseEntity.status(HttpStatus.CREATED).body(event);
		
		
	}
	@GetMapping("/v1/getevent")
	public ResponseEntity<Event> getSample( ){
		//Invoke Kafka producer
		Event event=new Event();
		event.setEventId(1);
		Book book=new Book();
		book.setBookAuthor("KT");
		book.setBookId(1);
		book.setBookName("Karnakar");
		event.setBook(book);
		
		
		return ResponseEntity.status(HttpStatus.CREATED).body(event);
		
	}
	
	

}
