package com.kt.learnkafka.kafkaproducer.producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kt.learnkafka.kafkaproducer.domian.Book;
import com.kt.learnkafka.kafkaproducer.domian.Event;
import com.kt.learnkafka.kafkaproducer.domian.EventType;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.producer.ProducerRecord;
@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerTest {
	
	@InjectMocks
	LibraryEventProducer libraryEventProducer;
	
	@Mock
	KafkaTemplate<Integer, String> kafkaTemplate;
	@Spy
	ObjectMapper objectMapper;
	@Test
	void testsendLibraryEvent_Approach2_Failure() throws JsonProcessingException {
		//given
		Book book = Book.builder().bookId(1).bookName("Name").bookAuthor("Auth").build();
		Event event = Event.builder().eventId(2).book(book).ventType(EventType.NEW).build();

		//when
		SettableListenableFuture future = new SettableListenableFuture();

        future.setException(new RuntimeException("Exception Calling Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
        //when

        assertThrows(Exception.class, ()->libraryEventProducer.sendLibraryEvent_Approach2(event).get());
		libraryEventProducer.sendLibraryEvent_Approach2(event);
		//then
	}
}
