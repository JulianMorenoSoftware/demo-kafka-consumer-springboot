package com.example.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

/**
 * Verifica de extremo a extremo que un {@link GreetingEvent} publicado por
 * {@link GreetingProducer} es recibido por un consumidor del topic de
 * saludos, usando el broker embebido de {@code spring-kafka-test} en lugar
 * de un Kafka real.
 *
 * <p>{@link GreetingListener} solo registra los eventos recibidos en el log
 * y no expone ningún estado observable desde un test, por lo que no puede
 * usarse directamente para verificar la recepción. En vez de modificar esa
 * clase de producción con código solo-de-test, este test registra un
 * consumidor auxiliar ({@link GreetingCapturingListener}) suscrito al mismo
 * topic mediante {@link CapturingListenerConfig}, con su propio {@code
 * group-id} para no competir por particiones con el listener de
 * producción dentro del mismo grupo de consumidores.</p>
 */
@SpringBootTest
@Import(GreetingKafkaIntegrationTest.CapturingListenerConfig.class)
@EmbeddedKafka(partitions = 1, topics = "${app.kafka.topics.greetings}")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class GreetingKafkaIntegrationTest {

    @Autowired
    private GreetingProducer greetingProducer;

    @Autowired
    private GreetingCapturingListener capturingListener;

    @Test
    void publishedGreetingIsConsumedByListener() {
        GreetingEvent event = GreetingEvent.of("ana", "hola kafka");

        greetingProducer.send(event);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(capturingListener.lastReceived()).isEqualTo(event));
    }

    /**
     * Registra el bean {@link GreetingCapturingListener} únicamente para
     * este test, evitando que el consumidor auxiliar forme parte del
     * contexto de producción de la aplicación.
     */
    @TestConfiguration
    static class CapturingListenerConfig {

        @Bean
        GreetingCapturingListener greetingCapturingListener() {
            return new GreetingCapturingListener();
        }
    }

    /**
     * Consumidor auxiliar de solo-test que se suscribe al topic de saludos
     * con un {@code group-id} propio y guarda el último {@link
     * GreetingEvent} recibido para que el test pueda aserirlo, ya que
     * {@link GreetingListener} no expone ningún estado observable.
     */
    static class GreetingCapturingListener {

        private final AtomicReference<GreetingEvent> lastReceived = new AtomicReference<>();

        /**
         * Almacena el evento recibido para su posterior verificación desde
         * el test.
         *
         * @param event evento de saludo recibido del topic bajo prueba
         */
        @KafkaListener(topics = "${app.kafka.topics.greetings}", groupId = "greeting-capture-test-group")
        void capture(GreetingEvent event) {
            lastReceived.set(event);
        }

        /**
         * Devuelve el último {@link GreetingEvent} capturado, o
         * {@code null} si todavía no se ha recibido ninguno.
         *
         * @return el último evento recibido, o {@code null}
         */
        GreetingEvent lastReceived() {
            return lastReceived.get();
        }
    }
}
