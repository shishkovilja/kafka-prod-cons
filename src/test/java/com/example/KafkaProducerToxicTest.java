package com.example;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class KafkaProducerToxicTest extends KafkaToxicTestBase {
    /** */
    public KafkaProducerToxicTest() {
        log = LoggerFactory.getLogger(KafkaProducerToxicTest.class);
    }

    /** */
    @ParameterizedTest(name = "timeout={0}")
    @ValueSource(longs = {HALF_TIMEOUT, TIMEOUT + HALF_TIMEOUT, TIMEOUT * 2 + HALF_TIMEOUT,
        TOTAL_TIMEOUT + HALF_TIMEOUT})
    @Timeout(TEST_TIMEOUT)
    public void producerSendTimeouts(long timeout) throws Exception {
        log.info(">>>>>> Starting producer");

        Properties props = producerProperties();

        try (var producer = new KafkaProducer<Long, Long>(props)) {
            AtomicLong val = new AtomicLong();

            send(producer, val, TOTAL_TIMEOUT);

            startDelays();

            Class<? extends Exception> cls = timeout > TOTAL_TIMEOUT ? ExecutionException.class
                : TimeoutException.class;

            Exception ex = assertThrows(cls, () -> send(producer, val, timeout));

            if (ex instanceof ExecutionException)
                assertTrue(ex.getCause() instanceof org.apache.kafka.common.errors.TimeoutException);

            log.info(">>>>>> Expected exception occurred:", ex);

            stopDelays();

            send(producer, val, timeout);
        }
    }

    /** */
    protected void send(KafkaProducer<Long, Long> producer, AtomicLong keyVal, long timeout) throws Exception {
        log.info(">>>>>> Sending keyVal={}", keyVal.incrementAndGet());

        Future<RecordMetadata> fut = producer.send(new ProducerRecord<>(TOPIC, keyVal.get(), keyVal.get()));

        fut.get(timeout, MILLISECONDS);

        log.info(">>>>>> keyVal={} was successfully sent", keyVal);
    }
}
