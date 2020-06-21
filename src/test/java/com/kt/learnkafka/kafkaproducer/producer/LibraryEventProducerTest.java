package com.kt.learnkafka.kafkaproducer.producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kt.learnkafka.kafkaproducer.domian.Book;
import com.kt.learnkafka.kafkaproducer.domian.Event;
import com.kt.learnkafka.kafkaproducer.domian.EventType;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
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
	
	@Test
	void testsendLibraryEvent_Approach2_Sucess() throws JsonProcessingException, InterruptedException, ExecutionException {
		//given
		Book book = Book.builder().bookId(1).bookName("Name").bookAuthor("Auth").build();
		Event event = Event.builder().eventId(2).book(book).ventType(EventType.NEW).build();

		//when
		SettableListenableFuture future = new SettableListenableFuture();
		ProducerRecord<Integer, String> prodRecord=new ProducerRecord("event", event.getEventId(),objectMapper.writeValueAsString(event));
		
		RecordMetadata recordMetadata=new RecordMetadata(new TopicPartition("event",1),1,1,342,System.currentTimeMillis(),1,2);
		SendResult<Integer, String> sendResult=new SendResult<Integer, String>(prodRecord,recordMetadata);
        future.set(sendResult);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
        //when

        ListenableFuture<SendResult<Integer, String>> sendLibraryEvent_Approach2 = libraryEventProducer.sendLibraryEvent_Approach2(event);
		//then
        SendResult<Integer, String> sendResult2 = sendLibraryEvent_Approach2.get();
        assert sendResult2.getRecordMetadata().partition()==1;
        
        
	}
}
