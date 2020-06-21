package com.kt.learnkafka.kafkaproducer.domian;

import javax.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Event {
	private EventType ventType;
	private Integer eventId;
	@NotNull
	
	private Book book;

}
