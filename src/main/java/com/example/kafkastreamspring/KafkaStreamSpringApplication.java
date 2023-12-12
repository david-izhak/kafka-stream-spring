package com.example.kafkastreamspring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaStreamSpringApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamSpringApplication.class, args);
	}

}
