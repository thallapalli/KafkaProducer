package com.kt.learnkafka.kafkaproducer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.Timed;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kt.learnkafka.kafkaproducer.domian.Book;
import com.kt.learnkafka.kafkaproducer.domian.Event;
import com.kt.learnkafka.kafkaproducer.domian.EventType;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = { "event" })
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",

		"spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"

})
class KafkaProducerApplicationTests {
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
	void contextLoads() {
	}

	@Test
	@Timeout(5)
	public void testpostEvent() {
		Book book = Book.builder().bookId(1).bookName("Name").bookAuthor("Auth").build();
		Event event = Event.builder().eventId(2).book(book).ventType(EventType.NEW).build();

		HttpHeaders httpHeader = new HttpHeaders();
		httpHeader.set("contect-type", MediaType.APPLICATION_JSON_VALUE);
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

}
