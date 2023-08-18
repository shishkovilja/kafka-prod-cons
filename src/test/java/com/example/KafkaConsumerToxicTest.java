package com.example;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.example.ConsumerAction.*;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 *
 */
public class KafkaConsumerToxicTest extends KafkaToxicTestBase {
    /** */
    public KafkaConsumerToxicTest() {
        log = LoggerFactory.getLogger(KafkaConsumerToxicTest.class);
    }

    /** */
    private static Stream<Arguments> params() {
        return Arrays.stream(ConsumerAction.values())
                .flatMap(a -> IntStream.range(0, 4)
                    .mapToObj(i -> Arguments.of(a, i * TIMEOUT + HALF_TIMEOUT)));
    }

    /** */
    @ParameterizedTest(name = "action={0}, timeout={1}")
    @MethodSource("params")
    @Timeout(TEST_TIMEOUT)
    public void testTimeout(ConsumerAction action, long timeout) throws Exception {
        log.info(">>>>>> Start consumer");

        try (KafkaConsumer<Long, Long> cnsmr = new KafkaConsumer<>(consumerProperties())) {
            cnsmr.subscribe(singletonList(TOPIC));

            // Necessary before commit.
            if (action == COMMIT_ASYNC || action == COMMIT_SYNC)
                cnsmr.poll(Duration.ofMillis(1000));

            startDelays();

            // Poll and async commit are not failing.
            if (action == POLL || action == COMMIT_ASYNC)
                action.accept(cnsmr, timeout);
            else {
                TimeoutException ex = assertThrows(TimeoutException.class, () -> action.accept(cnsmr, timeout));

                log.info(">>>>>> Expected exception was thrown: ", ex);

                stopDelays();

                action.accept(cnsmr, timeout);
            }

            log.info(">>>>>> After action");
        }
    }

    /** */
    @ParameterizedTest(name = "timeout={0}")
    @ValueSource(longs = {HALF_TIMEOUT, TIMEOUT + HALF_TIMEOUT, TIMEOUT * 2 + HALF_TIMEOUT,
        TOTAL_TIMEOUT + HALF_TIMEOUT})
    @Timeout(TEST_TIMEOUT)
    public void pollWithDelay_thenCommit(long timeout) throws Exception {
        log.info(">>>>>> Start consumer");

        try (KafkaConsumer<Long, Long> cnsmr = new KafkaConsumer<>(consumerProperties())) {
            cnsmr.subscribe(singletonList(TOPIC));

            startDelays();

            log.info(">>>>>> Before poll");
            POLL.accept(cnsmr, timeout);
            log.info(">>>>>> After poll");

            stopDelays();

            log.info(">>>>>> Before commit");
            COMMIT_SYNC.accept(cnsmr, timeout);
            log.info(">>>>>> After commit");
        }
    }
}
