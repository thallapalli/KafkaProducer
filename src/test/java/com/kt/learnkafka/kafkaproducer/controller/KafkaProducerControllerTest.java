package com.kt.learnkafka.kafkaproducer.controller;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kt.learnkafka.kafkaproducer.domian.Book;
import com.kt.learnkafka.kafkaproducer.domian.Event;
import com.kt.learnkafka.kafkaproducer.domian.EventType;
import com.kt.learnkafka.kafkaproducer.producer.LibraryEventProducer;
import static org.mockito.Mockito.when;
@WebMvcTest(KafkaProducerController.class)
@AutoConfigureMockMvc
public class KafkaProducerControllerTest {
	@Autowired
	MockMvc mockkMvc;
	ObjectMapper objMapper = new ObjectMapper();
	@MockBean
	LibraryEventProducer libraryEventProducer;

	@Test
	void postEventTest() throws Exception {
		Book book = Book.builder().bookId(1).bookName("Name").bookAuthor("Auth").build();
		Event event = Event.builder().eventId(2).book(book).ventType(EventType.NEW).build();
		String payload = objMapper.writeValueAsString(event);
		//doNothing().when(libraryEventProducer).sendLibraryEvent_Approach2(isA(Event.class));
		 when(libraryEventProducer.sendLibraryEvent_Approach2(isA(Event.class))).thenReturn(null);

		mockkMvc.perform(post("/v1/event").content(payload).contentType(MediaType.APPLICATION_JSON)

		).andExpect(status().isCreated());

	}

	@Test
	void postEventTest_4XX() throws Exception {
		 Book book = Book.builder()
	                .bookId(null)
	                .bookAuthor(null)
	                .bookName("Kafka using Spring Boot")
	                .build();

	        Event event = Event.builder()
	                .eventId(null)
	                .book(null)
	                .build();
	        String erroMesg="book must not be null";
		String payload = objMapper.writeValueAsString(event);
		 when(libraryEventProducer.sendLibraryEvent_Approach2(isA(Event.class))).thenReturn(null);

			
		mockkMvc.perform(post("/v1/event").content(payload).contentType(MediaType.APPLICATION_JSON)

		).andExpect(status().is4xxClientError())
		
		.andExpect(content().string(erroMesg))
		;

	}
	


}
