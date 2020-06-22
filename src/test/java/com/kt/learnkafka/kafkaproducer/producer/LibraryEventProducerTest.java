package com.kt.learnkafka.kafkaproducer.producer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kt.learnkafka.kafkaproducer.domian.Book;
import com.kt.learnkafka.kafkaproducer.domian.Event;
import com.kt.learnkafka.kafkaproducer.domian.EventType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
@ExtendWith(MockitoExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)

@EmbeddedKafka(topics = { "event" })
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",

		"spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"

})
public class LibraryEventProducerTest {
	
	@InjectMocks
	LibraryEventProducer libraryEventProducer;
	
	@Mock
	KafkaTemplate<Integer, String> kafkaTemplate;
	@Spy
	ObjectMapper objectMapper;
	@Autowired
	TestRestTemplate testRestTempalte;
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	@Autowired
	ObjectMapper objMapper;

	private Consumer<Integer, String> consumer;

	@BeforeEach
	void setUp() {
		// String group, String autoCommit,
		// EmbeddedKafkaBroker embeddedKafka

		Map<String, Object> configs = new HashMap<>(
				KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
		Deserializer keyDeserializerSupplier = new IntegerDeserializer();
		Deserializer valueDeserializerSupplier = new StringDeserializer();
		consumer = new DefaultKafkaConsumerFactory<>(configs, keyDeserializerSupplier, valueDeserializerSupplier)
				.createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);

	}

	@AfterEach
	void tearDown() {
		consumer.close();
	}
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
	@Test
	@Timeout(5)
	public void testpostEvent() {
		Book book = Book.builder().bookId(1).bookName("Name").bookAuthor("Auth").build();
		Event event = Event.builder().eventId(2).book(book).ventType(EventType.NEW).build();

		HttpHeaders httpHeader = new HttpHeaders();
		httpHeader.set("contentt-type", MediaType.APPLICATION_JSON_VALUE);
		HttpEntity<Event> request = new HttpEntity<>(event, httpHeader);
		ResponseEntity<Event> exchange = testRestTempalte.exchange("/v1/event", HttpMethod.POST, request, Event.class);
		assertEquals(HttpStatus.CREATED, exchange.getStatusCode());
		ConsumerRecord<Integer, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, "event");

		try {
			

			String expected = objMapper.writeValueAsString(event);
			String value = singleRecord.value();
			assertEquals(expected, value);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		

	}
	@Test
	   @Timeout(5)
	   void putLibraryEvent() throws InterruptedException, JsonProcessingException {
	       //given
	       Book book = Book.builder()
	               .bookId(456)
	               .bookAuthor("KT")
	               .bookName("Kafka using Spring Boot")
	               .build();

	       Event libraryEvent = Event.builder()
	               .eventId(123).ventType(EventType.UPDATE)
	               .book(book)
	               .build();
	       HttpHeaders headers = new HttpHeaders();
	       headers.set("content-type", MediaType.APPLICATION_JSON.toString());
	       HttpEntity<Event> request = new HttpEntity<>(libraryEvent, headers);

	       //when
	       ResponseEntity<Event> responseEntity = testRestTempalte.exchange("/v1/event", HttpMethod.PUT, request, Event.class);

	       //then
	       assertEquals(HttpStatus.OK, responseEntity.getStatusCode());

	       ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "event");
	       //Thread.sleep(3000);
	       String expected = objMapper.writeValueAsString(libraryEvent);
			 String value = consumerRecord.value();
	       assertEquals(expected, value);

	   }

	
	
}
