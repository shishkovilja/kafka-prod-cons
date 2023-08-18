package com.example;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testcontainers.utility.DockerImageName.parse;

/**
 *
 */
@SuppressWarnings("resource")
@Testcontainers
public abstract class KafkaToxicTestBase {
    /** Timeout. */
    protected static final int TEST_TIMEOUT = 60_000;

    /** Topic. */
    protected static final String TOPIC = "test";

    /** Request timeout. */
    protected static final int TIMEOUT = 10_000;

    /** Half timeout. */
    protected static final int HALF_TIMEOUT = TIMEOUT / 2;

    /** Producer delivery timeout ms. */
    protected static final int TOTAL_TIMEOUT = 3 * TIMEOUT;

    /** Toxic port. */
    protected static final int TOXIC_PORT = 9993;

    /** Kafka port. */
    protected static final int KAFKA_PORT = 9093;

    /** Network. */
    private static final Network net = Network.newNetwork();

    /** Test name thread context key. */
    private static final String TEST_NAME = "TEST_NAME";

    /** Logger. */
    protected Logger log = LoggerFactory.getLogger(KafkaToxicTestBase.class);

    /** Toxiproxy container. */
    @Container
    protected static final ToxiproxyContainer toxiproxy = new ToxiproxyContainer(parse("ghcr.io/shopify/toxiproxy:2.5.0"))
        .withExposedPorts(8474, TOXIC_PORT)
        .withNetwork(net);

    /** Kafka container. */
    @Container
    protected static final KafkaContainer kafka = new KafkaContainer(parse("confluentinc/cp-kafka:6.2.10")) {
        @Override public String getBootstrapServers() {
            // Overriding value from KafkaContainer mapped port by mapped toxic port.
            return String.format("PLAINTEXT://%s:%s", getHost(), toxiproxy.getMappedPort(TOXIC_PORT));
        }
    }
        .dependsOn(toxiproxy)
        .withNetwork(net)
        .withNetworkAliases("kafka");

    /** Proxy. */
    protected static Proxy proxy;

    /** */
    @BeforeAll
    protected static void beforeAll() throws Exception {
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
    void setUp(TestInfo testInfo) {
        String testCls = testInfo.getTestClass()
                .orElse(KafkaToxicTestBase.class)
                .getSimpleName();

        String testMethod = testInfo.getTestMethod()
                .map(Method::getName)
                .orElse("");

        String displayName = testInfo.getDisplayName().replace('=', '_');

        String testName = String.join("_", testCls, testMethod, displayName);

        System.setProperty(TEST_NAME, testName);
    }

    /** */
    @AfterEach
    protected void tearDown() throws Exception {
        for (Toxic toxic : proxy.toxics().getAll())
            toxic.remove();

        System.clearProperty(TEST_NAME);
    }

    /** */
    protected Properties consumerProperties() {
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
    protected Properties producerProperties() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, TIMEOUT * 2);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, TOTAL_TIMEOUT);

        return props;
    }

    /** */
    protected void startDelays() throws IOException {
        log.info(">>>>>> Start delays");

        proxy.toxics().latency("latency", ToxicDirection.DOWNSTREAM,
            TIMEOUT + HALF_TIMEOUT);
    }


    /** */
    protected void stopDelays() throws IOException {
        log.info(">>>>>> Stop delays");

        proxy.toxics().get("latency")
            .remove();
    }
}
