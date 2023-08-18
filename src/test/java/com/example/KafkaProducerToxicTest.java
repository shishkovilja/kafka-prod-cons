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
import org.junit.jupiter.api.function.Executable;
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

        try (KafkaProducer<Long, Long> producer = new KafkaProducer<>(props)) {
            AtomicLong val = new AtomicLong();

            send(producer, val, TOTAL_TIMEOUT);

            startDelays();

            boolean isTotalTimeout = timeout == TOTAL_TIMEOUT + HALF_TIMEOUT;

            Class<? extends Exception> cls = isTotalTimeout ? ExecutionException.class : TimeoutException.class;

            Executable executable = isTotalTimeout ? () -> send(producer, val, -1) : () -> send(producer, val, timeout);

            long start = System.currentTimeMillis();
            Exception ex = assertThrows(cls, executable);
            long execTime = System.currentTimeMillis() - start;

            log.info(">>>>>> Execution time: " + execTime);

            if (ex instanceof ExecutionException) {
                assertTrue(ex.getCause() instanceof org.apache.kafka.common.errors.TimeoutException);
                assertTrue(execTime < timeout);
            }

            log.info(">>>>>> Expected exception occurred:", ex);

            stopDelays();

            send(producer, val, isTotalTimeout ? -1 : timeout);
        }
    }

    /** */
    protected void send(KafkaProducer<Long, Long> producer, AtomicLong keyVal, long timeout) throws Exception {
        log.info(">>>>>> Sending keyVal={}", keyVal.incrementAndGet());

        Future<RecordMetadata> fut = producer.send(new ProducerRecord<>(TOPIC, keyVal.get(), keyVal.get()));

        if (timeout < 0)
            fut.get();
        else
            fut.get(timeout, MILLISECONDS);

        log.info(">>>>>> keyVal={} was successfully sent", keyVal);
    }
}
