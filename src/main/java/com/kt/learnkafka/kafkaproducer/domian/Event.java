package com.kt.learnkafka.kafkaproducer.domian;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Event {
	private EventType ventType ;
	private Integer eventId;
	private Book book;
	

}
