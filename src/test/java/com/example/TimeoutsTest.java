package com.example;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class TimeoutsTest {
    /** Timeout. */
    public static final int TEST_TIMEOUT = 60_000;

    /** Topic. */
    private static final String TOPIC = "test";

    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(ProdConsTest.class);

    /** Request timeout ms. */
    public static final int REQUEST_TIMEOUT_MS = 10_000;

    /** Producer linger ms. */
    public static final int LINGER_MS = 1000;

    /** Producer delivery timeout ms. */
    public static final int TOTAL_TIMEOUT_MS = 3 * (REQUEST_TIMEOUT_MS + LINGER_MS);

    /** Logger marker. */
    private Marker logMarker;

    /** Kafka. */
    private EmbeddedKafkaCluster kafka;

    /** */
    @BeforeEach
    public void setUp() throws Exception {
        logMarker = null;

        kafka = initKafka(kafka);
    }

    /** */
    @AfterEach
    public void tearDown() throws Exception {
        if (kafka != null)
            kafka.deleteAllTopicsAndWait(TEST_TIMEOUT);
    }

    /** */
    @ParameterizedTest(name = "timeout={0}")
    @ValueSource(longs = {REQUEST_TIMEOUT_MS / 2, REQUEST_TIMEOUT_MS, REQUEST_TIMEOUT_MS * 2,
        TOTAL_TIMEOUT_MS + REQUEST_TIMEOUT_MS / 2})
    @Timeout(TEST_TIMEOUT)
    public void producerSendTimeouts(long timeout) throws Exception {
        logMarker = MarkerFactory.getMarker("producerSendTimeouts, timeout=" + timeout);

        log.info(logMarker, ">>>>>> Starting producer");

        Properties props = producerProperties();

        KafkaProducer<Long, Long> producer = new KafkaProducer<>(props);

        AtomicLong val = new AtomicLong();

        send(producer, val, TOTAL_TIMEOUT_MS);

        log.info(logMarker, ">>>>>> Stopping Kafka cluster");

        kafka.stop();
        kafka = null;

        log.info(logMarker, ">>>>>> Kafka cluster stopped");

        Class<? extends Exception> cls = timeout > TOTAL_TIMEOUT_MS ? ExecutionException.class
            : TimeoutException.class;

        Exception ex = assertThrows(cls, () -> send(producer, val, timeout));

        if (ex instanceof ExecutionException)
            assertTrue(ex.getCause() instanceof org.apache.kafka.common.errors.TimeoutException);

        log.info(logMarker, ">>>>>> Expected exeption occurred:", ex);

        producer.close();
    }

    /** */
    @ParameterizedTest(name = "timeout={0}")
    @ValueSource(longs = {REQUEST_TIMEOUT_MS / 2, REQUEST_TIMEOUT_MS, REQUEST_TIMEOUT_MS * 2,
        TOTAL_TIMEOUT_MS + REQUEST_TIMEOUT_MS / 2})
    @Timeout(TEST_TIMEOUT)
    public void consumerPollTimeout(long timeout) {
        logMarker = MarkerFactory.getMarker("consumerPollTimeout, timeout=" + timeout);

        log.info(logMarker, ">>>>>> Starting consumer");

        Properties props = consumerProperties();

        KafkaConsumer<Long, Long> consumer = new KafkaConsumer<>(props);

//        consumer.subscribe(Collections.singleton(TOPIC));
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC, 0)));

        consumer.poll(Duration.ofMillis(1000));

        log.info(logMarker, ">>>>>> Stopping Kafka cluster");

        kafka.stop();
        kafka = null;

        log.info(logMarker, ">>>>>> Kafka cluster stopped");

        int idx = 0;

        while (idx < timeout / 1000) {
            log.info(logMarker, ">>>>>> Poll idx: " + ++idx);

            consumer.poll(Duration.ofMillis(1000));
        }

        consumer.close();
    }

    /** */
    @ParameterizedTest(name = "timeout={0}")
    @ValueSource(longs = {REQUEST_TIMEOUT_MS / 2, REQUEST_TIMEOUT_MS, REQUEST_TIMEOUT_MS * 2,
        TOTAL_TIMEOUT_MS + REQUEST_TIMEOUT_MS / 2})
    @Timeout(TEST_TIMEOUT)
    public void commitTimeout(long timeout) {
        logMarker = MarkerFactory.getMarker("consumerPollTimeout, timeout=" + timeout);

        log.info(logMarker, ">>>>>> Starting consumer");

        Properties props = consumerProperties();

        KafkaConsumer<Long, Long> consumer = new KafkaConsumer<>(props);

        consumer.assign(Collections.singleton(new TopicPartition(TOPIC, 0)));

        consumer.poll(Duration.ofMillis(1000));

        log.info(logMarker, ">>>>>> Stopping Kafka cluster");

        kafka.stop();
        kafka = null;

        log.info(logMarker, ">>>>>> Kafka cluster stopped");

        consumer.commitSync(Duration.ofMillis(timeout));

        consumer.close();
    }

    /** */
    private void send(KafkaProducer<Long, Long> producer, AtomicLong keyVal, long timeout) throws Exception {
        log.info(logMarker, ">>>>>> Sending keyVal={}", keyVal.incrementAndGet());

        Future<RecordMetadata> fut = producer.send(new ProducerRecord<>("test", keyVal.get(), keyVal.get()));

        fut.get(timeout, MILLISECONDS);
    }

    /** */
    private Properties consumerProperties() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TOTAL_TIMEOUT_MS);

        return props;
    }

    /** */
    private Properties producerProperties() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        props.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, TOTAL_TIMEOUT_MS);

        return props;
    }

    /** */
    private EmbeddedKafkaCluster initKafka(EmbeddedKafkaCluster curKafka) throws IOException, InterruptedException {
        if (curKafka == null) {
            curKafka = new EmbeddedKafkaCluster(1);

            curKafka.start();
        }

        curKafka.createTopic(TOPIC);
        curKafka.waitForRemainingTopics(TEST_TIMEOUT, TOPIC);

        return curKafka;
    }
}
