package com.kt.learnkafka.kafkaproducer.controller;

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
import com.kt.learnkafka.kafkaproducer.producer.LibraryEventProducer;

@RestController
public class KafkaProducerController {
	@Autowired
	LibraryEventProducer libraryEventProducer;
	
	@PostMapping("/v1/event")
	public ResponseEntity<Event> postEvent(@RequestBody Event event ) throws JsonProcessingException{
		//Invoke Kafka producer
		libraryEventProducer.sendLibraryEvent(event);
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
