package com.example.kafka.messaging;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Evento inmutable que solicita la generación de una secuencia numérica del
 * 1 hasta {@code upperBound}.
 *
 * <p>Se modela como {@code record} porque es un objeto de valor sin
 * identidad propia. La generación de la secuencia se coloca en el propio
 * evento en vez de en una clase "helper" externa, ya que es comportamiento
 * directamente asociado a su único dato: evita una clase anémica.</p>
 *
 * @param upperBound límite superior (inclusive) de la secuencia a generar; debe ser positivo
 */
public record CountRequestEvent(int upperBound) {

    /**
     * Valida que el límite superior solicitado sea un entero positivo, ya
     * que una secuencia de 1 a N solo tiene sentido para N &gt;= 1.
     *
     * @throws IllegalArgumentException si {@code upperBound} no es positivo
     */
    public CountRequestEvent {
        if (upperBound <= 0) {
            throw new IllegalArgumentException("upperBound debe ser un entero positivo, recibido: " + upperBound);
        }
    }

    /**
     * Genera la secuencia numérica del 1 hasta {@link #upperBound}, ambos
     * inclusive.
     *
     * @return lista ordenada con los números del 1 a {@code upperBound}
     */
    public List<Integer> sequence() {
        return IntStream.rangeClosed(1, upperBound).boxed().toList();
    }
}
