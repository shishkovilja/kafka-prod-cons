package com.example;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.utility.DockerImageName.parse;

/**
 *
 */
@SuppressWarnings("resource") @Testcontainers
public class KafkaTimeoutsToxicTest {
    /** Timeout. */
    public static final int TEST_TIMEOUT = 60_000;

    /** Topic. */
    private static final String TOPIC = "test";

    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(ProdConsTest.class);

    /** Request timeout. */
    public static final int TIMEOUT = 10_000;

    /** Half timeout. */
    public static final int HALF_TIMEOUT = TIMEOUT / 2;

    /** Producer delivery timeout ms. */
    public static final int TOTAL_TIMEOUT = 3 * TIMEOUT;

    /** Toxic port. */
    public static final int TOXIC_PORT = 9993;

    /** Kafka port. */
    public static final int KAFKA_PORT = 9093;

    /** Logger marker. */
    private Marker logMarker;

    /** Network. */
    private static final Network net = Network.newNetwork();

    /** Toxiproxy container. */
    @Container
    private static final ToxiproxyContainer toxiproxy = new ToxiproxyContainer(parse("ghcr.io/shopify/toxiproxy:2.5.0"))
        .withExposedPorts(8474, TOXIC_PORT)
        .withNetwork(net);

    /** Kafka container. */
    @Container
    private static final KafkaContainer kafka = new KafkaContainer(parse("confluentinc/cp-kafka:6.2.10")) {
        @Override public String getBootstrapServers() {
            // Overriding value from KafkaContainer mapped port by mapped toxic port.
            return String.format("PLAINTEXT://%s:%s", getHost(), toxiproxy.getMappedPort(TOXIC_PORT));
        }
    }
        .dependsOn(toxiproxy)
        .withNetwork(net)
        .withNetworkAliases("kafka");

    /** Proxy. */
    private static Proxy proxy;

    /** */
    @BeforeAll
    static void beforeAll() throws Exception {
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        proxy = toxiproxyClient.createProxy("kafka", "0.0.0.0:" + TOXIC_PORT, "kafka:" + KAFKA_PORT);

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        AdminClient admin = KafkaAdminClient.create(adminProps);

        admin.createTopics(Collections.singleton(new NewTopic(TOPIC, 1, (short)1)))
            .all().get(TEST_TIMEOUT, MILLISECONDS);

        admin.close();
    }

    /** */
    @BeforeEach
    public void setUp() {
        logMarker = null;
    }

    /** */
    @AfterEach
    void tearDown() throws Exception {
        for (Toxic toxic : proxy.toxics().getAll())
            toxic.remove();
    }

    /** */
    private static Stream<Arguments> params() {
        return Arrays.stream(Action.values())
                .flatMap(a -> IntStream.range(0, 4)
                    .mapToObj(i -> Arguments.of(a, i * TIMEOUT + HALF_TIMEOUT)));
    }

    /** */
    @ParameterizedTest(name = "timeout={0}")
    @ValueSource(longs = {HALF_TIMEOUT, TIMEOUT + HALF_TIMEOUT, TIMEOUT * 2 + HALF_TIMEOUT,
        TOTAL_TIMEOUT + HALF_TIMEOUT})
    @Timeout(TEST_TIMEOUT)
    public void producerSendTimeouts(long timeout) throws Exception {
        logMarker = MarkerFactory.getMarker("producerSendTimeouts, timeout=" + timeout);

        log.info(logMarker, ">>>>>> Starting producer");

        Properties props = producerProperties();

        KafkaProducer<Long, Long> producer = new KafkaProducer<>(props);

        AtomicLong val = new AtomicLong();

        send(producer, val, TOTAL_TIMEOUT);

        log.info(logMarker, ">>>>>> Stopping Kafka cluster");

        kafka.stop();

        log.info(logMarker, ">>>>>> Kafka cluster stopped");

        Class<? extends Exception> cls = timeout > TOTAL_TIMEOUT ? ExecutionException.class
            : TimeoutException.class;

        Exception ex = assertThrows(cls, () -> send(producer, val, timeout));

        if (ex instanceof ExecutionException)
            assertTrue(ex.getCause() instanceof org.apache.kafka.common.errors.TimeoutException);

        log.info(logMarker, ">>>>>> Expected exception occurred:", ex);

        producer.close();
    }

    /** */
    @ParameterizedTest(name = "timeout={0}")
    @ValueSource(longs = {HALF_TIMEOUT, TIMEOUT + HALF_TIMEOUT, TIMEOUT * 2 + HALF_TIMEOUT,
        TOTAL_TIMEOUT + HALF_TIMEOUT})
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

        log.info(logMarker, ">>>>>> Kafka cluster stopped");

        int idx = 0;

        while (idx < timeout / 1000) {
            log.info(logMarker, ">>>>>> Poll idx: " + ++idx);

            consumer.poll(Duration.ofMillis(1000));
        }

        consumer.close();
    }

    /** */
    @ParameterizedTest(name = "action={0}, timeout={1}")
    @MethodSource("params")
    @Timeout(TEST_TIMEOUT)
    public void testTimeout(Action action, long timeout) throws Exception {
        logMarker = MarkerFactory.getMarker("action=" + action + ", timeout=" + timeout);

        log.info(logMarker, ">>>>>> Start consumer");

        try (var cnsmr = new KafkaConsumer<Long, Long>(consumerProperties())) {
            cnsmr.subscribe(List.of(TOPIC));

            // Necessary before commit.
            if (action == Action.COMMIT)
                cnsmr.poll(Duration.ofMillis(1000));

            log.info(logMarker, ">>>>>> Start delays");

            proxy.toxics().latency("latency", ToxicDirection.DOWNSTREAM,
                TIMEOUT + HALF_TIMEOUT);

            assertThrows(org.apache.kafka.common.errors.TimeoutException.class,
                () -> action.accept(cnsmr, timeout));

            log.info(logMarker, ">>>>>> Stop delays");

            proxy.toxics().get("latency")
                .remove();

            action.accept(cnsmr, timeout);

            log.info(logMarker, ">>>>>> After action");
        }
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

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TOTAL_TIMEOUT);

        // Necessary,because delays of heartbeats lead to group rebalance.
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, TOTAL_TIMEOUT * 2);

        return props;
    }

    /** */
    private Properties producerProperties() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, TOTAL_TIMEOUT);

        return props;
    }

    /**
     * Actions performed over consumer with specified timeout.
     */
    private enum Action {
        /** Commit. */
        COMMIT((cnsmr, t) -> cnsmr.commitSync(Duration.ofMillis(t))),

        /** Partitions. */
        PARTITIONS((cnsmr, t) -> cnsmr.partitionsFor(TOPIC, Duration.ofMillis(t))),

        /** End offsets. */
        END_OFFSETS((cnsmr, t) -> cnsmr.endOffsets(List.of(new TopicPartition(TOPIC, 0)), Duration.ofMillis(t)));

        /** Action. */
        private final BiConsumer<KafkaConsumer<?, ?>, Long> action;

        /** */
        Action(BiConsumer<KafkaConsumer<?, ?>, Long> action) {
            this.action = action;
        }

        /** */
        public void accept(KafkaConsumer<?, ?> cnsmr, long timeout) {
            action.accept(cnsmr, timeout);
        }
    }
}
