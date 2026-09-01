# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

This is a freshly scaffolded Spring Boot project (Spring Initializr output) intended to demonstrate Apache Kafka / Kafka Streams integration. As of now it contains no business logic — only the default `DemoKafkaApplication` bootstrap class and a placeholder context-loading test. There are no Kafka listeners, producers, topologies, or configuration beyond the application name.

## Build & run commands

Use the Maven wrapper (`mvnw`), not a system-installed Maven, so the correct Maven version is used.

```bash
./mvnw compile              # compile
./mvnw test                 # run all tests
./mvnw test -Dtest=DemoKafkaApplicationTests   # run a single test class
./mvnw test -Dtest=DemoKafkaApplicationTests#contextLoads  # run a single test method
./mvnw spring-boot:run       # run the application locally
./mvnw clean package         # build the executable jar (target/*.jar)
```

## Stack

- Java 17, Spring Boot 3.5.16 (via `spring-boot-starter-parent`)
- Dependencies: `spring-boot-starter`, `spring-kafka`, `kafka-streams`
- Test dependencies: `spring-boot-starter-test`, `spring-kafka-test` (JUnit 5)
- Base package: `com.example.kafka`
- Config: `src/main/resources/application.yaml` (currently only sets `spring.application.name`)

Since `spring-kafka` and `kafka-streams` are already on the classpath, any Kafka work added to this project (consumers, producers, `KStream` topologies, `@KafkaListener`s) should be wired through Spring's Kafka auto-configuration and externalized via `application.yaml` (broker address, consumer group, topics, serdes) rather than hardcoded — this is idiomatic for how Spring Kafka expects configuration and keeps local vs. other environments swappable.

A running Kafka broker (e.g. via Docker) is needed to exercise any Kafka-connected code manually; `spring-kafka-test` provides embedded-broker support for integration tests without one.
