package com.example.kafka.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consume {@link CountRequestEvent} publicados en el topic de secuencias
 * numéricas y muestra por consola la secuencia resultante.
 *
 * <p>El listener delega en la auto-configuración de Spring Kafka
 * ({@code @KafkaListener}) para la gestión del {@code ConsumerFactory} y del
 * contenedor de escucha, evitando configuración manual salvo la
 * externalizada en {@code application.yaml} (topic, group id,
 * deserializers, etc.). La generación de la secuencia en sí vive en
 * {@link CountRequestEvent#sequence()}; este listener se limita a
 * orquestar la recepción y la impresión, manteniendo una única
 * responsabilidad.</p>
 */
@Component
public class CountListener {

    private static final Logger log = LoggerFactory.getLogger(CountListener.class);

    /**
     * Procesa un {@link CountRequestEvent} recibido del topic configurado
     * en {@code app.kafka.topics.count}, imprimiendo por consola cada
     * número de la secuencia solicitada.
     *
     * @param event evento con el límite superior de la secuencia a generar
     */
    @KafkaListener(topics = "${app.kafka.topics.count}")
    public void onCountRequest(CountRequestEvent event) {
        log.info("Secuencia numérica solicitada: 1 a {}", event.upperBound());
        event.sequence().forEach(number -> log.info("{}", number));
    }
}
