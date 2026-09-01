package com.example.kafka.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consume los {@link GreetingEvent} publicados en el topic de saludos.
 *
 * <p>El listener delega en la auto-configuración de Spring Kafka
 * ({@code @KafkaListener}) para la gestión del {@code ConsumerFactory} y del
 * contenedor de escucha, evitando configuración manual salvo la
 * externalizada en {@code application.yaml} (group id, deserializers,
 * etc.).</p>
 */
@Component
public class GreetingListener {

    private static final Logger log = LoggerFactory.getLogger(GreetingListener.class);

    /**
     * Procesa un {@link GreetingEvent} recibido del topic configurado en
     * {@code app.kafka.topics.greetings}.
     *
     * @param event evento de saludo recibido
     */
    @KafkaListener(topics = "${app.kafka.topics.greetings}")
    public void onGreeting(GreetingEvent event) {
        log.info("Saludo recibido de '{}': {} ({})", event.sender(), event.message(), event.createdAt());
    }
}
