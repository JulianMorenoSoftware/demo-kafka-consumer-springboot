package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Punto de entrada de la aplicación de demostración de Kafka.
 *
 * <p>{@link ConfigurationPropertiesScan} habilita el registro automático de
 * las clases de propiedades tipadas (como
 * {@link com.example.kafka.messaging.KafkaTopicsProperties}) sin necesidad de
 * declararlas explícitamente como {@code @Bean}.</p>
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class DemoKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoKafkaApplication.class, args);
	}

}
