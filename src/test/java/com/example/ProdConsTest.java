package com.example;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.Assert.assertFalse;

/** */
public class ProdConsTest {
    /** Topic. */
    private static final String TOPIC = "test";

    /** Max iterations. */
    private static final int MAX_ITERS = 10;

    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(ProdConsTest.class);

    /** Request timeout. */
    private static final int REQ_TIMEOUT = 3000;

    /** Brokers string. */
    private static final String BROKERS_STRING = "localhost:9092";

    /** Admin. */
    private static AdminClient admin;

    /** Logger marker. */
    private Marker logMarker;

    /** */
    @Before
    public void beforeTest() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS_STRING);

        admin = KafkaAdminClient.create(adminProps);

        admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short)1)));
    }

    /** */
    @After
    @SuppressWarnings("BusyWait")
    public void afterTest() throws Exception {
        admin.deleteTopics(List.of(TOPIC));

        do {
            Thread.sleep(500);

            Set<String> topics = admin.listTopics().names().get(3, SECONDS);

            if (topics.isEmpty())
                return;
        } while (true);
    }

    /** */
    @AfterClass
    public static void afterClass() {
        admin.close();
    }

    /** */
    @Test
    public void testWithSendBeforeCommit() throws Exception {
        logMarker = MarkerFactory.getMarker("testWithSendBeforeCommit");

        doTest(true, false, false);
    }

    /** */
    @Test
    public void testWithoutSendBeforeCommit() throws Exception {
        logMarker = MarkerFactory.getMarker("testWithoutSendBeforeCommit");

        doTest(false, false, false);
    }

    /** */
    @Test
    public void testZeroTimeoutOnFirstPoll() throws Exception {
        logMarker = MarkerFactory.getMarker("testZeroTimeoutOnFirstPoll");

        doTest(true, true, false);
    }

    /** */
    @Test
    public void testWithAllowRetry() throws Exception {
        logMarker = MarkerFactory.getMarker("testWithAllowRetry");

        doTest(false, true, true);
    }

    /** */
    private void doTest(boolean sendBeforeCommit, boolean zeroTimeoutOnFirstPoll, boolean allowRetryOfPoll) throws Exception {
        log.info(logMarker, ">>>>>> Starting test with parameters: sendBeforeCommit={}, zeroTimeoutOnFirstPoll={}, allowRetryOfPoll={}",
            sendBeforeCommit, zeroTimeoutOnFirstPoll, allowRetryOfPoll);

        try (KafkaConsumer<Long, Long> consumer = consumer(); KafkaProducer<Long, Long> producer = producer()) {
            log.info(logMarker, ">>>>>> Assigning partition");

            consumer.assign(List.of(new TopicPartition(TOPIC, 0)));

            AtomicLong keyVal = new AtomicLong();
            long receivedCnt = 0;
            long iterCnt = 0;

            while (iterCnt++ < MAX_ITERS) {
                send(producer, keyVal);

                sleep();

                int consumerTimeout = !zeroTimeoutOnFirstPoll && receivedCnt == 0 ? REQ_TIMEOUT : 0;

                ConsumerRecords<Long, Long> records;

                long pollIter = 0;

                do {
                    log.info(logMarker, ">>>>>> Polling with consumer: [iterCnt={}, pollIter={}, timeout={}]", iterCnt, ++pollIter,
                        consumerTimeout);

                    records = consumer.poll(Duration.ofMillis(consumerTimeout));

                    receivedCnt += records.count();

                    log.info(logMarker, ">>>>>> Got records: [iterCnt={}, current={}, total={}]", iterCnt, records.count(), receivedCnt);
                } while (records.isEmpty() && allowRetryOfPoll);

                assertFalse("Empty records obtained: iterCnt=" + iterCnt, records.isEmpty());

                if (sendBeforeCommit)
                    send(producer, keyVal);

                log.info(logMarker, ">>>>>> Comitting offsets: iterCnt={}", iterCnt);

                consumer.commitSync(Duration.ofMillis(REQ_TIMEOUT));

                sleep();
            }
        }
    }

    /** */
    private void sleep() throws InterruptedException {
        log.info(logMarker, ">>>>>> Sleeping a while...");
        Thread.sleep(2000);
    }

    /** */
    private void send(KafkaProducer<Long, Long> producer, AtomicLong keyVal) throws Exception {
        log.info(logMarker, ">>>>>> Sending keyVal={}", keyVal.incrementAndGet());

        Future<RecordMetadata> fut = producer.send(new ProducerRecord<>("test", keyVal.get(), keyVal.get()));

        fut.get(REQ_TIMEOUT, MILLISECONDS);
    }

    /** */
    private KafkaConsumer<Long, Long> consumer() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS_STRING);
        props.put(GROUP_ID_CONFIG, "test_group");
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        log.info(logMarker, ">>>>>> Starting consumer");

        return new KafkaConsumer<>(props);
    }

    /** */
    private KafkaProducer<Long, Long> producer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS_STRING);
        props.put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        log.info(logMarker, ">>>>>> Starting producer");

        return new KafkaProducer<>(props);
    }
}
