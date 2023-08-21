package com.example;

import com.example.expiry.ExpiredFilterFactory;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
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
import java.util.Collections;
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
@SuppressWarnings({"resource", "deprecation", "BusyWait"})
@Testcontainers
public class IgniteToxicTest {
    /** */
    private static final Logger log = LoggerFactory.getLogger(IgniteToxicTest.class);

    /** Empty string. */
    private static final String[] EMPTY_STRING = new String[0];

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
                    .withFixedExposedPort(TOXIC_THIN_PORT + 2, TOXIC_THIN_PORT + 2)
                    .withNetwork(NET);

    /** Ignite image version. */
    private static final String IGNITE_IMAGE = "apacheignite/ignite:2.14.0";

    /**
     * Cache loading threads count.
     */
    private static final int CACHE_LOADING_THREADS_CNT = 20;

    /** Cache loading duration. */
    private static final int CACHE_LOADING_DURATION = 60_000;

    /** Slow clients count. */
    private static final int SLOW_CLIENTS_CNT = 1;

    /** Proxies. */
    private static List<Proxy> proxies;

    /** */
    @Container
    private final GenericContainer<?> toxicIgnite0 = igniteContainer(0);

    /** */
    @Container
    private final GenericContainer<?> toxicIgnite1 = igniteContainer(1)
                    .dependsOn(toxicIgnite0)
                    .waitingFor(Wait.forLogMessage(".+Topology snapshot.+", 1));

    /** */
    @Container
    private final GenericContainer<?> toxicIgnite2 = igniteContainer(2)
            .dependsOn(toxicIgnite0)
            .waitingFor(Wait.forLogMessage(".+Topology snapshot.+", 1));

    /** Cache loader client. */
    private IgniteClient cacheLdrClient;

    /** Put futures. */
    private List<CompletableFuture<?>> putFuts;

    /** Put latch. */
    private CountDownLatch putLatch;

    /** Expiry moment (moment of all put entries expiration). */
    private long expiryMoment;

    /** Puts counter. */
    private AtomicInteger putsCnt;

    /**
     * @param idx Ignite index.
     */
    private static GenericContainer<?> igniteContainer(int idx) {
        return new GenericContainer<>(IGNITE_IMAGE)
                .withNetworkAliases("toxicIgnite" + idx)
                .withExposedPorts(THIN_PORT)
                .withNetwork(NET)
                .withClasspathResourceMapping("config.xml", "/config.xml", BindMode.READ_ONLY)
                .withFileSystemBind("work", "/work", BindMode.READ_WRITE)
                .withFileSystemBind("target", "/opt/ignite/apache-ignite/libs/user_libs", BindMode.READ_WRITE)
                .withEnv(asMap("JVM_OPTS", IGNITE_JVM_OPTS,
                        "CONFIG_URI", "/config.xml",
                        "IGNITE_WORK_DIR", "/work"))
                .withLogConsumer(new Slf4jLogConsumer(log));
    }

    /** */
    @BeforeAll
    protected static void beforeAll() {
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(TOXI_PROXY.getHost(), TOXIPROXY_CONTROL_PORT);

        proxies = range(0, 3)
                .mapToObj(i -> proxy(toxiproxyClient, i))
                .collect(Collectors.toList());
    }

    /** */
    private static Proxy proxy(ToxiproxyClient toxiproxyClient, int igniteIdx) {
        try {
            return toxiproxyClient.createProxy(
                    "toxicIgnite" + igniteIdx,
                    "0.0.0.0:" + (TOXIC_THIN_PORT + igniteIdx),
                    "toxicIgnite" + igniteIdx + ":" + THIN_PORT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    @BeforeEach
    public void before() {
        cacheLdrClient = igniteClient();

        while (cacheLdrClient.cluster().nodes().size() < 3) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        cacheLdrClient.cluster().state(ClusterState.ACTIVE);

        putsCnt = new AtomicInteger();

        putLatch = new CountDownLatch(CACHE_LOADING_THREADS_CNT);

        expiryMoment = System.currentTimeMillis() + CACHE_LOADING_DURATION;

        putFuts = range(0, CACHE_LOADING_THREADS_CNT)
                .mapToObj(i -> startCacheLoading(cacheLdrClient, putsCnt, putLatch))
                .collect(Collectors.toList());
    }

    /** */
    @AfterEach
    public void after() throws Exception {
        for (Proxy proxy : proxies) {
            for (Toxic toxic : proxy.toxics().getAll())
                toxic.remove();
        }

        cacheLdrClient.close();
    }

    /** */
    @Test
    @Timeout(TEST_TIMEOUT)
    void testContinuousQuery() throws Exception {
        AtomicInteger listenerFinishedCnt = new AtomicInteger();

        range(0, SLOW_CLIENTS_CNT).forEach(i ->
                igniteClient().cache(TEST_CACHE)
                        .query(query(listenerFinishedCnt)));

        waitForPuts();

        // Thin protocol bandwidth limited 10kB/sec -> increase in order to prevent node failure
        for (Proxy proxy : proxies)
            proxy.toxics().bandwidth(proxy.getName() + "-band-down", ToxicDirection.DOWNSTREAM, 10);

        // Waiting CQ to be processed
        while (true) {
            Thread.sleep(5000);

            assertEquals(3, cacheLdrClient.cluster()
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
    private IgniteClient igniteClient() {
        List<String> addrs = range(0, 3)
                .mapToObj(i -> TOXI_PROXY.getHost() + ":" + (TOXIC_THIN_PORT + i))
                .collect(Collectors.toList());

        Collections.shuffle(addrs);

        return Ignition.startClient(new ClientConfiguration()
                .setAddresses(addrs.toArray(EMPTY_STRING))
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
