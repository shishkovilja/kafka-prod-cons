package com.example;

import com.example.expiry.ExpiredFilterFactory;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.internal.util.lang.GridFunc.asMap;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 */
@SuppressWarnings({"resource", "deprecation"})
@Testcontainers
public class IgniteToxicTest {
    /**
     *
     */
    private static final Logger log = LoggerFactory.getLogger(IgniteToxicTest.class);

    /** Test timeout. */
    public static final int TEST_TIMEOUT = 600;

    /** Payload. */
    private static final byte[] PAYLOAD = new byte[4096];

    /** Ignite jvm opts. */
    private static final String IGNITE_JVM_OPTS = "-XX:+UseG1GC " +
            "-Xms512m " +
            "-Xmx512m " +
            "-XX:+ExitOnOutOfMemoryError " +
            "-XX:ErrorFile=/work/%p-err.log " +
            "-DIGNITE_QUIET=false " +
            "-DIGNITE_QUIET=false";

    /** Test cache. */
    private static final String TEST_CACHE = "test_cache";

    /** Thin port. */
    private static final int THIN_PORT = 10800;

    /** Toxic thin port for slow clients. */
    private static final Integer TOXIC_THIN_PORT = 11800;

    /** Toxiproxy control port. */
    private static final int TOXIPROXY_CONTROL_PORT = 8474;

    /** Network. */
    private static final Network NET = Network.newNetwork();

    /** Toxiproxy container. */
    @Container
    private static final FixedHostPortGenericContainer<?> TOXI_PROXY =
            new FixedHostPortGenericContainer<>("ghcr.io/shopify/toxiproxy:2.5.0")
                    .withFixedExposedPort(TOXIPROXY_CONTROL_PORT, TOXIPROXY_CONTROL_PORT)
                    .withFixedExposedPort(TOXIC_THIN_PORT, TOXIC_THIN_PORT)
                    .withFixedExposedPort(TOXIC_THIN_PORT + 1, TOXIC_THIN_PORT + 1)
                    .withNetwork(NET);

    /** Ignite image version. */
    private static final String IGNITE_IMAGE = "apacheignite/ignite:2.14.0";

    /**
     * Cache loading threads count.
     */
    private static final int CACHE_LOADING_THREADS_CNT = 20;

    /** Slow clients count. */
    private static final int SLOW_CLIENTS_CNT = 1;

    /** */
    @Container
    private final GenericContainer<?> toxicIgnite = new GenericContainer<>(IGNITE_IMAGE)
            .withNetworkAliases("toxicIgnite")
            .withExposedPorts(THIN_PORT)
            .withNetwork(NET)
            .withClasspathResourceMapping("config.xml", "/config.xml", BindMode.READ_ONLY)
            .withFileSystemBind("work", "/work", BindMode.READ_WRITE)
            .withFileSystemBind("target", "/opt/ignite/apache-ignite/libs/user_libs", BindMode.READ_WRITE)
            .withEnv(asMap("JVM_OPTS", IGNITE_JVM_OPTS,
                    "CONFIG_URI", "/config.xml",
                    "IGNITE_WORK_DIR", "/work"))
            .withLogConsumer(new Slf4jLogConsumer(log));

    /** */
    @Container
    private final GenericContainer<?> goodIgnite = new GenericContainer<>(IGNITE_IMAGE)
                    .withNetworkAliases("goodIgnite")
                    .withExposedPorts(THIN_PORT)
                    .withNetwork(NET)
                    .withClasspathResourceMapping("config.xml", "/config.xml", BindMode.READ_ONLY)
                    .withFileSystemBind("work", "/work", BindMode.READ_WRITE)
                    .withFileSystemBind("target", "/opt/ignite/apache-ignite/libs/user_libs", BindMode.READ_WRITE)
                    .withEnv(asMap("JVM_OPTS", IGNITE_JVM_OPTS,
                            "CONFIG_URI", "/config.xml",
                            "IGNITE_WORK_DIR", "/work"))
                    .withLogConsumer(new Slf4jLogConsumer(log))
                    .dependsOn(toxicIgnite)
                    .waitingFor(Wait.forLogMessage(".+Topology snapshot.+", 1));

    /** */
    @Container
    private final GenericContainer<?> otherGoodIgnite = new GenericContainer<>(IGNITE_IMAGE)
                    .withExposedPorts(THIN_PORT)
                    .withNetwork(NET)
                    .withClasspathResourceMapping("config.xml", "/config.xml", BindMode.READ_ONLY)
                    .withFileSystemBind("work", "/work", BindMode.READ_WRITE)
                    .withFileSystemBind("target", "/opt/ignite/apache-ignite/libs/user_libs", BindMode.READ_WRITE)
                    .withEnv(asMap("JVM_OPTS", IGNITE_JVM_OPTS,
                            "CONFIG_URI", "/config.xml",
                            "IGNITE_WORK_DIR", "/work"))
                    .withLogConsumer(new Slf4jLogConsumer(log))
                    .dependsOn(toxicIgnite)
                    .waitingFor(Wait.forLogMessage(".+Topology snapshot.+", 1));

    /** Proxy. */
    private static Proxy proxy;

    /** Good client. */
    private IgniteClient goodClient;

    /** Put futures. */
    private List<CompletableFuture<?>> putFuts;

    /** Put latch. */
    private CountDownLatch putLatch;

    /** Expiry moment (moment of all put entries expiration). */
    private long expiryMoment;

    /** Puts counter. */
    private AtomicInteger putsCnt;

    /** */
    @BeforeAll
    protected static void beforeAll() throws Exception {
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(TOXI_PROXY.getHost(), TOXIPROXY_CONTROL_PORT);
        proxy = toxiproxyClient.createProxy("toxicIgnite", "0.0.0.0:" + TOXIC_THIN_PORT, "toxicIgnite:" + THIN_PORT);

        // Good proxy
        toxiproxyClient.createProxy("goodIgnite", "0.0.0.0:" + (TOXIC_THIN_PORT + 1), "goodIgnite:" + THIN_PORT);
    }

    /** */
    @BeforeEach
    public void before() {
        goodClient = igniteClient(goodIgnite.getHost(), TOXIC_THIN_PORT + 1);

        while (goodClient.cluster().nodes().size() < 3) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        goodClient.cluster().state(ClusterState.ACTIVE);

        putsCnt = new AtomicInteger();

        putLatch = new CountDownLatch(CACHE_LOADING_THREADS_CNT);

        expiryMoment = System.currentTimeMillis() + 60_000;

        putFuts = range(0, CACHE_LOADING_THREADS_CNT)
                .mapToObj(i -> startCacheLoading(goodClient, putsCnt, putLatch))
                .collect(Collectors.toList());
    }

    /** */
    @AfterEach
    public void after() {
        goodClient.close();
    }

    /** */
    @Test
    @Timeout(TEST_TIMEOUT)
    void testContinuousQuery() throws Exception {
        AtomicInteger listenerFinishedCnt = new AtomicInteger();

        range(0, SLOW_CLIENTS_CNT).forEach(i ->
                igniteClient(TOXI_PROXY.getHost(), TOXIC_THIN_PORT)
                        .cache(TEST_CACHE)
                        .query(query(listenerFinishedCnt)));

        waitForPuts();

        // Thin protocol bandwidth limited 100kB/sec -> increase in order to prevent node failure
        proxy.toxics().bandwidth("toxicIgniteDown", ToxicDirection.DOWNSTREAM, 10);

        // Waiting CQ to be processed
        while (true) {
            Thread.sleep(5000);

            assertEquals(3, goodClient.cluster()
                    .forServers()
                    .nodes()
                    .size(), "Size of cluster should not change");

            int cnt = listenerFinishedCnt.get();

            log.warn(">>>>>> Listened: " + cnt);

            if (cnt >= SLOW_CLIENTS_CNT * putsCnt.get())
                break;
        }
    }

    /**
     *
     */
    private void waitForPuts() throws InterruptedException, ExecutionException {
        assertTrue(putLatch.await(TEST_TIMEOUT, SECONDS), "Puts not finished");

        for (CompletableFuture<?> completableFuture : putFuts)
            completableFuture.get();

        log.warn(">>>>>> After puts (putsCnt=" + putsCnt.get() + "). Expiration will start soon...");
    }

    /**
     * @param listenerFinishedCnt Listener latch.
     */
    private ContinuousQuery<Object, Object> query(AtomicInteger listenerFinishedCnt) {
        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setIncludeExpired(true);

        // In order to use run `mvn -DskipTests clean package`
        qry.setRemoteFilterFactory(new ExpiredFilterFactory());

        qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> cacheEntryEvents)
                    throws CacheEntryListenerException {
                cacheEntryEvents.forEach(e -> {
                    int cnt = listenerFinishedCnt.incrementAndGet();

                    if (cnt % 1_000 == 0)
                        log.warn(">>>>>> Listened count: " + cnt);
                });
            }
        });

        return qry;
    }

    /**
     *
     */
    private IgniteClient igniteClient(String host, int port) {
        return Ignition.startClient(new ClientConfiguration()
                .setAddresses(host + ':' + port)
                .setPartitionAwarenessEnabled(true)
        );
    }

    /**
     *
     */
    private CompletableFuture<?> startCacheLoading(IgniteClient client, AtomicInteger putsCnt,
                                                   CountDownLatch putLatch) {
        ClientCache<Object, Object> cache = client.cache(TEST_CACHE);

        return CompletableFuture.runAsync(() -> {
            long duration;

            while ((duration = expiryMoment - System.currentTimeMillis()) > 2_000) {
                try {
                    cache.withExpirePolicy(
                                    // All entries will expire at the same time
                                    new CreatedExpiryPolicy(new Duration(MILLISECONDS, duration))
                            )
                            .put(UUID.randomUUID(), PAYLOAD);

                    int cnt = putsCnt.incrementAndGet();

                    if (cnt % 1000 == 0)
                        log.warn(">>>>>> Puts performed: " + cnt);
                } catch (Exception e) {
                    fail("Error occurred: " + e.getMessage());
                }
            }

            putLatch.countDown();
        });
    }
}
