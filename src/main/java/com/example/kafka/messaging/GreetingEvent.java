package com.example.kafka.messaging;

import java.time.Instant;

/**
 * Evento inmutable que representa un saludo enviado a través de Kafka.
 *
 * <p>Se modela como {@code record} porque es un objeto de valor: una vez
 * creado no cambia, y su igualdad se basa exclusivamente en sus datos. Se
 * serializa a JSON tanto en el productor como en el consumidor mediante los
 * serializadores de Spring Kafka configurados en {@code application.yaml}.</p>
 *
 * @param sender    nombre de quien origina el saludo
 * @param message   contenido del saludo
 * @param createdAt instante en el que se generó el evento
 */
public record GreetingEvent(String sender, String message, Instant createdAt) {

    /**
     * Crea un {@link GreetingEvent} asignando automáticamente el instante
     * de creación al momento actual.
     *
     * @param sender  nombre de quien origina el saludo
     * @param message contenido del saludo
     * @return un nuevo evento con {@code createdAt} igual a {@link Instant#now()}
     */
    public static GreetingEvent of(String sender, String message) {
        return new GreetingEvent(sender, message, Instant.now());
    }
}
