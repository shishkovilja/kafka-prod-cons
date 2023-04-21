package com.example;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import static com.example.ConsumerAction.COMMIT;
import static com.example.ConsumerAction.POLL;
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

        try (var cnsmr = new KafkaConsumer<Long, Long>(consumerProperties())) {
            cnsmr.subscribe(List.of(TOPIC));

            // Necessary before commit.
            if (action == ConsumerAction.COMMIT)
                cnsmr.poll(Duration.ofMillis(1000));

            startDelays();

            // Poll is not failing.
            if (action == POLL)
                POLL.accept(cnsmr, timeout);
            else {
                assertThrows(org.apache.kafka.common.errors.TimeoutException.class,
                    () -> action.accept(cnsmr, timeout));

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

        try (var cnsmr = new KafkaConsumer<Long, Long>(consumerProperties())) {
            cnsmr.subscribe(List.of(TOPIC));

            startDelays();

            log.info(">>>>>> Before poll");
            POLL.accept(cnsmr, timeout);
            log.info(">>>>>> After poll");

            stopDelays();

            log.info(">>>>>> Before commit");
            COMMIT.accept(cnsmr, timeout);
            log.info(">>>>>> After commit");
        }
    }
}
