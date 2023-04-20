package com.example;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.apache.ignite.cache.CachePeekMode.PRIMARY;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
@SuppressWarnings({"resource", "deprecation"}) @Testcontainers
public class TestIgnite {
    /** */
    private static final Logger log = LoggerFactory.getLogger(TestIgnite.class);

    /** Test cache. */
    public static final String TEST_CACHE = "test_cache";

    /** */
    @Container
    private final FixedHostPortGenericContainer<?> ignite0 = new FixedHostPortGenericContainer<>("apacheignite/ignite")
        .withFixedExposedPort(10800, 10800)
        .withEnv(Map.of("JVM_OPTS", "-DIGNITE_QUIET=false"))
        .withLogConsumer(new Slf4jLogConsumer(log));

    /** */
    @Container
    private final FixedHostPortGenericContainer<?> ignite1 = new FixedHostPortGenericContainer<>("apacheignite/ignite")
        .withFixedExposedPort(10801, 10800)
        .withEnv(Map.of("JVM_OPTS", "-DIGNITE_QUIET=false"))
        .withLogConsumer(new Slf4jLogConsumer(log))
        .dependsOn(ignite0)
        .waitingFor(Wait.forLogMessage(".+Topology snapshot.+", 1));

    /** */
    @Test
    void testIngniteShutdown() throws Exception {
        IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(
            ignite1.getHost() + ':' + ignite1.getMappedPort(10800),
            ignite0.getHost() + ':' + ignite0.getMappedPort(10800)));

        assertEquals(2, client.cluster().nodes().size());

        AtomicLong errors = new AtomicLong();

        var fut = startCacheLoading(client, errors);

        // Waiting, that puts performed
        Thread.sleep(1000);

        var cache = client.cache(TEST_CACHE);
        long curSize = 0;

        // Check for errors and increased size
        assertTrue(curSize < (curSize = cache.size(PRIMARY)));
        assertEquals(0, errors.get());

        log.warn(">>>>>> Stopping node");
        ignite1.stop();

        // Ensure node failing, see log for NODE_FAILED exceptions.
        Thread.sleep(DFLT_FAILURE_DETECTION_TIMEOUT + DFLT_FAILURE_DETECTION_TIMEOUT / 2);

        // Check for errors and increased size
        assertTrue(curSize < (curSize = cache.size(PRIMARY)));
        assertEquals(0, errors.get());

        ignite1.start();

        assertEquals(2, client.cluster().nodes().size());

        fut.cancel(true);

        // Check for errors and increased size
        assertTrue(curSize < cache.size(PRIMARY));
        assertEquals(0, errors.get());
    }


    /** */
    @SuppressWarnings("BusyWait")
    private static CompletableFuture<?> startCacheLoading(IgniteClient client, AtomicLong errors) {
        var cache = client.getOrCreateCache(new ClientCacheConfiguration()
            .setName(TEST_CACHE)
            .setBackups(1));

        return CompletableFuture.runAsync(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(100);

                    cache.put(UUID.randomUUID(), UUID.randomUUID());
                }
                catch (ClientConnectionException e) {
                    errors.incrementAndGet();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new RuntimeException(e);
                }
            }
        });
    }
}
