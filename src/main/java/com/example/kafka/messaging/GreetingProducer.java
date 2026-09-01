package com.example.kafka.messaging;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publica {@link GreetingEvent} en el topic de saludos configurado.
 *
 * <p>Se apoya en el {@link KafkaTemplate} auto-configurado por Spring Boot a
 * partir de {@code spring.kafka.*} en {@code application.yaml}, por lo que no
 * es necesario construir manualmente un {@code ProducerFactory}. El nombre
 * del topic se obtiene de {@link KafkaTopicsProperties} en lugar de estar
 * hardcodeado.</p>
 */
@Service
public class GreetingProducer {

    private final KafkaTemplate<String, GreetingEvent> kafkaTemplate;
    private final KafkaTopicsProperties topicsProperties;

    /**
     * Crea el productor inyectando sus dependencias por constructor.
     *
     * @param kafkaTemplate    plantilla de Kafka auto-configurada por Spring Boot
     * @param topicsProperties propiedades con los nombres de topic de la aplicación
     */
    public GreetingProducer(KafkaTemplate<String, GreetingEvent> kafkaTemplate,
                             KafkaTopicsProperties topicsProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicsProperties = topicsProperties;
    }

    /**
     * Envía un {@link GreetingEvent} al topic de saludos, usando el nombre
     * del remitente como clave de partición para que todos los saludos de
     * un mismo remitente conserven orden relativo.
     *
     * @param event evento a publicar
     */
    public void send(GreetingEvent event) {
        kafkaTemplate.send(topicsProperties.greetings(), event.sender(), event);
    }
}
