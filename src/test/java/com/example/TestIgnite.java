package com.example;

import java.util.Map;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
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

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 */
@Testcontainers
public class TestIgnite {
    /** */
    private static final Logger log = LoggerFactory.getLogger(TestIgnite.class);

    /** */
    @Container
    private final FixedHostPortGenericContainer<?> ignite0 = new FixedHostPortGenericContainer<>("apacheignite/ignite")
        .withFixedExposedPort(10800, 10800)
        .withEnv(Map.of("JVM_OPTS", "-DIGNITE_QUIET=false"/*, "CONFIG_URI", "/config.xml"*/))
//        .withClasspathResourceMapping("/config.xml", "/config.xml", BindMode.READ_ONLY)
        .withLogConsumer(new Slf4jLogConsumer(log));

    /** */
    @Container
    private final FixedHostPortGenericContainer<?> ignite1 = new FixedHostPortGenericContainer<>("apacheignite/ignite")
        .withFixedExposedPort(10801, 10800)
        .withEnv(Map.of("JVM_OPTS", "-DIGNITE_QUIET=false" /*, "CONFIG_URI", "/config.xml"*/))
//        .withClasspathResourceMapping("/config.xml", "/config.xml", BindMode.READ_ONLY)
        .withLogConsumer(new Slf4jLogConsumer(log))
        .dependsOn(ignite0)
        .waitingFor(Wait.forLogMessage(".+Topology snapshot.+", 1));


    /** */
    @Test
    void testIngniteShutdown() throws Exception {
        IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(
            ignite1.getHost() + ':' + 10800,
            ignite0.getHost() + ':' + 10801));

        assertEquals(2, client.cluster().nodes().size());

        var cache = populateCache(client);

        log.warn(">>>>>> Stopping node");
        ignite1.stop();

        Thread.sleep(DFLT_FAILURE_DETECTION_TIMEOUT);

        assertEquals("test", cache.get("test"));

        ignite1.start();

        assertEquals(2, client.cluster().nodes().size());
        assertEquals("test", cache.get("test"));
    }

    /** */
    private static ClientCache<Object, Object> populateCache(IgniteClient client) {
        var cache = client.getOrCreateCache(new ClientCacheConfiguration()
            .setName("test_cache")
            .setBackups(1));

        cache.put("test", "test");

        assertEquals("test", cache.get("test"));

        return cache;
    }
}
