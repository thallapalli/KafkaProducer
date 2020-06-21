package com.kt.learnkafka.kafkaproducer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import com.kt.learnkafka.kafkaproducer.domian.Book;
import com.kt.learnkafka.kafkaproducer.domian.Event;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)

class KafkaProducerApplicationTests {
	@Autowired 
	TestRestTemplate testRestTempalte;
	
	

	@Test
	void contextLoads() {
	}
	@Test
	public void  testpostEvent( ) {
		Book book=Book.builder().bookId(1).bookName("Name").bookAuthor("Auth").build();
		Event event=Event.builder().eventId(2).book(book).build();
		
		HttpHeaders httpHeader=new HttpHeaders();
		httpHeader.set("contect-type",MediaType.APPLICATION_JSON_VALUE);
		HttpEntity<Event> request=new HttpEntity<>(event,httpHeader);
		ResponseEntity<Event> exchange = testRestTempalte.exchange("/v1/event", HttpMethod.POST, request, Event.class);
		assertEquals(HttpStatus.CREATED, exchange.getStatusCode());
		
	}
	
}
