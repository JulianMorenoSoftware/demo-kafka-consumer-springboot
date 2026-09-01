package com.example.kafka.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Agrupa los nombres de los topics de Kafka usados por la aplicación.
 *
 * <p>Externalizar los nombres de topic en {@code application.yaml} bajo el
 * prefijo {@code app.kafka.topics} evita hardcodear cadenas mágicas en el
 * código de productores, consumidores o topologías, y permite variar el
 * nombre por entorno sin recompilar.</p>
 *
 * @param greetings nombre del topic donde se publican y consumen los {@link GreetingEvent}
 * @param count     nombre del topic donde se publican y consumen los {@link CountRequestEvent}
 */
@ConfigurationProperties(prefix = "app.kafka.topics")
public record KafkaTopicsProperties(String greetings, String count) {
}
