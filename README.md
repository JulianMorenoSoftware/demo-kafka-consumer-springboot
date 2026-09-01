# demo-kafka Consumidor

Aplicación Spring Boot que actúa como **consumidor de eventos** dentro de un ecosistema de demostración de Apache Kafka. Escucha *topics* de Kafka mediante `@KafkaListener` y procesa los eventos publicados por [`demo-kafka productor`](https://github.com/JulianMorenoSoftware/demo-kafka-producer-springboot).

Este README está pensado para que cualquier persona del equipo, sin contexto previo, entienda **qué hace este servicio, cómo se comunica con el resto del ecosistema, y cómo levantarlo localmente**.

## Repositorios relacionados

Este proyecto es una pieza de un ecosistema de 3 repositorios que se complementan entre sí:

| Repositorio | Rol | Enlace |
|---|---|---|
| `kafka-broker-docker` | Infraestructura: levanta el broker de Kafka en Docker | https://github.com/JulianMorenoSoftware/kafka-broker-docker |
| `demo-kafka producer` | Productor: expone APIs REST que publican eventos | https://github.com/JulianMorenoSoftware/demo-kafka-producer-springboot |
| **`demo-kafka consumer`** (este repo) | Consumidor: escucha los topics y procesa los eventos | https://github.com/JulianMorenoSoftware/demo-kafka-consumer-springboot |

## Arquitectura general

```
                     HTTP POST                         Kafka topic                    @KafkaListener
 Cliente/Postman/curl ────────► demo-kafka productor ────────────────► demo-kafka Consumidor (este repo)
                                  (puerto 8080)                          (puerto 8081)
                                        │                                      │
                                        └──────────► Kafka broker ◄────────────┘
                                                    (localhost:9092 por defecto)
```

1. Un cliente externo llama a un endpoint REST del servicio [**productor**](https://github.com/JulianMorenoSoftware/demo-kafka-producer-springboot).
2. El productor serializa un evento en JSON y lo publica en un topic de Kafka.
3. El broker de Kafka (ver [`kafka-broker-docker`](https://github.com/JulianMorenoSoftware/kafka-broker-docker)) persiste el mensaje.
4. **Este servicio** tiene `@KafkaListener`s suscritos a esos topics. Cuando llega un mensaje nuevo, Spring Kafka lo deserializa automáticamente al tipo de evento correspondiente e invoca el método anotado, que hoy simplemente lo registra en el log (es el punto de extensión natural para agregar lógica de negocio real más adelante: guardar en base de datos, notificar, disparar otro proceso, etc.).

Este servicio nunca llama directamente al productor ni sabe que existe — solo reacciona a lo que aparece en los topics que escucha. Ese desacoplamiento es la característica central de una arquitectura *event-driven*.

## Stack técnico

- **Java 17**
- **Spring Boot 3.5.16** (`spring-boot-starter-parent`)
- **spring-boot-starter-web** — expuesto aunque este servicio no define controllers propios (queda disponible para health checks / actuator si se agrega en el futuro)
- **spring-kafka** — integración de Spring con Kafka (`@KafkaListener`, configuración de consumidores)
- **kafka-streams** — dependencia declarada en el `pom.xml` para futuras topologías de streaming; **aún no se usa** en el código actual
- Tests: `spring-boot-starter-test`, `spring-kafka-test` (soporta `@EmbeddedKafka` para tests de integración sin broker real), `awaitility`

## Requisitos previos

Antes de ejecutar este servicio necesitas un broker de Kafka corriendo. Este proyecto **no** levanta Kafka por sí mismo — depende del repositorio de infraestructura:

👉 **[`kafka-broker-docker`](https://github.com/JulianMorenoSoftware/kafka-broker-docker)**

Pasos resumidos (ver el README de ese repo para el detalle completo):

```bash
git clone https://github.com/JulianMorenoSoftware/kafka-broker-docker.git
cd kafka-broker-docker
docker compose up -d
docker compose ps   # confirmar que "broker" y "kafka-ui" están "Up"
```

Esto levanta:
- El **broker de Kafka** (modo KRaft, sin Zookeeper), accesible en `localhost:9092` (o la IP del host, ver más abajo).
- **Kafka UI**, una interfaz web para inspeccionar topics y mensajes, en `http://localhost:9080`.

## Configuración (`application.yaml`)

```yaml
server:
  port: 8081

spring:
  application:
    name: demo-kafka
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: demo-kafka-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.example.kafka.messaging

app:
  kafka:
    topics:
      greetings: greetings-topic
      count: count-topic
```

Explicación propiedad por propiedad:

- `server.port: 8081`: puerto HTTP del servicio. Se fija explícitamente en 8081 para no chocar con el productor, que corre en el 8080 (por defecto). Este servicio no expone endpoints REST propios hoy; el puerto solo es relevante para health checks o futura administración vía Actuator.
- `spring.kafka.bootstrap-servers`: dirección `host:puerto` del broker de Kafka. Por defecto es `localhost:9092`, coherente con el puerto que expone `kafka-broker-docker` (`9092:9092`) cuando el broker corre en tu misma máquina. Debe apuntar al mismo broker que usa el productor. Si el broker corre en otra IP de tu red (real o de la empresa), **no edites este archivo**: sobrescribe la propiedad exportando la variable de entorno `SPRING_KAFKA_BOOTSTRAP_SERVERS=<ip-real>:9092` antes de levantar la aplicación (Spring Boot mapea automáticamente esa variable a `spring.kafka.bootstrap-servers`).
- `spring.kafka.consumer.group-id: demo-kafka-group`: identifica el *consumer group* al que pertenece esta instancia. Todos los listeners de esta app comparten el mismo group-id, lo que significa que si se levantan varias instancias de este servicio, Kafka repartirá las particiones entre ellas (escalado horizontal por partición) en lugar de duplicar el procesamiento.
- `spring.kafka.consumer.auto-offset-reset: earliest`: si este consumer group nunca leyó un topic antes (o su offset guardado ya no existe en el broker), empieza a leer **desde el mensaje más antiguo disponible**, en vez de solo los mensajes nuevos que lleguen a partir de ahora. Útil en demos, para no "perderse" eventos publicados antes de levantar el consumidor.
- `spring.kafka.consumer.key-deserializer` / `value-deserializer`: cómo se interpretan la clave y el valor de cada mensaje. Clave como `String`, valor como JSON.
- `spring.json.trusted.packages: com.example.kafka.messaging`: por seguridad, `JsonDeserializer` de Spring Kafka **no** deserializa a cualquier clase que venga en el header del mensaje — solo a clases del o los paquetes explícitamente marcados como confiables. Aquí se permite únicamente `com.example.kafka.messaging`, donde viven los records de eventos (`GreetingEvent`, `CountRequestEvent`).
- `app.kafka.topics.greetings` / `app.kafka.topics.count`: nombres de los topics a escuchar, externalizados en vez de quedar como *magic strings*. Se leen a través de `KafkaTopicsProperties` (`@ConfigurationProperties(prefix = "app.kafka.topics")`), registrada automáticamente por `@ConfigurationPropertiesScan` en `DemoKafkaApplication`.

## Modelo de eventos

| Evento (record Java) | Campos | Topic origen | Consumido por |
|---|---|---|---|
| `GreetingEvent` | `sender: String`, `message: String`, `createdAt: Instant` | `greetings-topic` | `GreetingListener` |
| `CountRequestEvent` | `upperBound: int` | `count-topic` | `CountListener` |

```java
// messaging/GreetingEvent.java
public record GreetingEvent(String sender, String message, Instant createdAt) {}

// messaging/CountRequestEvent.java
public record CountRequestEvent(int upperBound) {
    public CountRequestEvent {
        if (upperBound <= 0) {
            throw new IllegalArgumentException("upperBound debe ser un entero positivo, recibido: " + upperBound);
        }
    }
    public List<Integer> sequence() {
        return IntStream.rangeClosed(1, upperBound).boxed().toList();
    }
}
```

`CountRequestEvent` valida en su constructor compacto que `upperBound` sea positivo (falla rápido si llega un evento inválido), y expone `sequence()`, que genera la lista `[1, 2, ..., upperBound]`.

## Listeners de Kafka

### `GreetingListener`

Escucha `greetings-topic` (resuelto vía `${app.kafka.topics.greetings}`) y simplemente registra en el log el saludo recibido:

```java
@Component
public class GreetingListener {

    @KafkaListener(topics = "${app.kafka.topics.greetings}")
    public void onGreeting(GreetingEvent event) {
        log.info("Saludo recibido de '{}': {} ({})", event.sender(), event.message(), event.createdAt());
    }
}
```

Explicación línea a línea:
- `@KafkaListener(topics = "${app.kafka.topics.greetings}")`: le dice a Spring Kafka "suscribe este método al topic cuyo nombre está en la propiedad `app.kafka.topics.greetings`" (es decir, `greetings-topic`). Spring maneja automáticamente la suscripción, el polling del broker y la deserialización del mensaje a `GreetingEvent`.
- El parámetro `GreetingEvent event`: Spring ya te entrega el objeto deserializado, no el JSON crudo. No hay que parsear nada manualmente.
- El cuerpo del método solo hace `log.info(...)`: este es el punto donde, en un caso real, iría la lógica de negocio (guardar el saludo, notificar por otro canal, etc.). Hoy es un placeholder deliberado para el demo.

### `CountListener`

Escucha `count-topic` (resuelto vía `${app.kafka.topics.count}`) y, por cada `CountRequestEvent` recibido, imprime en el log la secuencia numérica de `1` hasta `upperBound`:

```java
@Component
public class CountListener {

    @KafkaListener(topics = "${app.kafka.topics.count}")
    public void onCountRequest(CountRequestEvent event) {
        log.info("Secuencia numérica solicitada: 1 a {}", event.upperBound());
        event.sequence().forEach(number -> log.info("{}", number));
    }
}
```

Explicación línea a línea:
- Igual que el listener anterior, `@KafkaListener` suscribe el método al topic `count-topic`.
- `event.sequence()` invoca el método del record `CountRequestEvent`, que genera la lista `1..upperBound`.
- Se recorre esa lista logueando cada número — es intencionalmente simple, para que el foco del demo sea el flujo de mensajería, no la lógica de negocio.

## Cómo se relaciona con el productor

El servicio [`demo-kafka productor`](https://github.com/JulianMorenoSoftware/demo-kafka-producer-springboot) (puerto **8080**) expone los endpoints REST que publican en los topics que este consumidor escucha:

- `POST /greetings/{sender}?message={message}` → publica en `greetings-topic` → recibido por `GreetingListener`.
- `POST /sequences/{name}` → publica en `count-topic` → **debería** ser recibido por `CountListener` (ver nota importante abajo).

Consulta el README del productor para el detalle de cada endpoint: https://github.com/JulianMorenoSoftware/demo-kafka-producer-springboot#readme

## ⚠️ Nota conocida: contrato desalineado en `count-topic`

Al revisar ambos proyectos se detectó que **el topic `count-topic` tiene productor y consumidor con contratos de evento incompatibles**:

- El productor publica `SequenceEvent(long value, Instant createdAt)` en `count-topic` (vía `POST /sequences/{name}`).
- Este servicio (`CountListener`) espera deserializar `CountRequestEvent(int upperBound)` desde ese mismo topic.

Como `spring-kafka` usa `JsonSerializer`/`JsonDeserializer`, el productor incluye en cada mensaje un header con el nombre completo de la clase Java de origen (`com.example.kafka.messaging.SequenceEvent`). **Este consumidor no tiene esa clase en su classpath** — solo conoce `CountRequestEvent` dentro de su propio paquete `com.example.kafka.messaging`. En consecuencia, si hoy alguien invoca `POST /sequences/{name}` en el productor mientras este consumidor está corriendo, la deserialización fallará del lado del consumidor y el mensaje quedará como error en su log, en vez de ser procesado por `CountListener`.

**Esto queda documentado deliberadamente, no oculto:** sirve como ejemplo real de que dos servicios pueden compilar perfectamente por separado y aun así romper su integración si el contrato de evento (nombre de clase y forma de los campos) no se coordina entre quien publica y quien consume. Antes de demostrar este flujo en vivo, hay que alinear ambos proyectos para que usen el mismo record de evento en `count-topic`.

El flujo de **`greetings-topic` sí es consistente** de punta a punta: mismo record `GreetingEvent` en ambos proyectos, por lo que la deserialización funciona sin problemas.

## 🔒 Nota de seguridad: no hardcodear IPs reales en este repo

Este repositorio es **público**. `spring.kafka.bootstrap-servers` usa `localhost:9092` como valor por defecto a propósito, en vez de una IP real de la red interna: el broker de Kafka de [`kafka-broker-docker`](https://github.com/JulianMorenoSoftware/kafka-broker-docker) corre en `PLAINTEXT`, sin autenticación ni cifrado, así que publicar la IP exacta de un broker real en un repo público facilita el reconocimiento de red a cualquiera que ya tenga acceso a esa LAN. Si necesitas apuntar a un broker en una IP específica, hazlo vía variable de entorno (`SPRING_KAFKA_BOOTSTRAP_SERVERS`) en tu entorno local, nunca commiteando el valor real en `application.yaml`.

## Cómo ejecutar todo el stack localmente

Orden recomendado de arranque:

1. **Levantar Kafka** (ver [`kafka-broker-docker`](https://github.com/JulianMorenoSoftware/kafka-broker-docker)):
   ```bash
   cd kafka-broker-docker
   docker compose up -d
   ```
2. **Levantar este consumidor** (para que esté escuchando antes de producir eventos):
   ```bash
   cd "demo-kafka Consumidor"
   ./mvnw spring-boot:run
   ```
3. **Levantar el productor**:
   ```bash
   cd "demo-kafka productor"
   ./mvnw spring-boot:run
   ```
4. **Probar el flujo de greetings** (extremo a extremo, funciona correctamente) desde otra terminal:
   ```bash
   curl -X POST "http://localhost:8080/greetings/juan?message=hola%20equipo"
   ```
   Deberías ver en los logs de **este** servicio una línea similar a:
   ```
   Saludo recibido de 'juan': hola equipo (2026-...)
   ```
5. (Opcional) Inspeccionar los mensajes publicados desde **Kafka UI**: http://localhost:9080

## Comandos Maven útiles

Usa siempre el wrapper (`./mvnw`), no una instalación de Maven del sistema, para garantizar la versión correcta:

```bash
./mvnw compile              # compilar
./mvnw test                 # ejecutar todos los tests
./mvnw test -Dtest=DemoKafkaApplicationTests            # ejecutar una clase de test específica
./mvnw test -Dtest=DemoKafkaApplicationTests#contextLoads  # ejecutar un método de test específico
./mvnw spring-boot:run       # ejecutar la aplicación localmente
./mvnw clean package         # generar el jar ejecutable (target/*.jar)
```

## Referencias

- Infraestructura de Kafka (Docker Compose): https://github.com/JulianMorenoSoftware/kafka-broker-docker
- Servicio productor: https://github.com/JulianMorenoSoftware/demo-kafka-producer-springboot
- Servicio consumidor (este repo): https://github.com/JulianMorenoSoftware/demo-kafka-consumer-springboot
