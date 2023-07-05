package com.example;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import static com.example.KafkaToxicTestBase.TOPIC;
import static java.time.Duration.ofMillis;

/**
 * Actions performed over consumer with specified timeout.
 */
public enum ConsumerAction {
    /** Poll. */
    POLL((cnsmr, t) -> cnsmr.poll(ofMillis(t))),

    /** Commit sync. */
    COMMIT_SYNC((cnsmr, t) -> cnsmr.commitSync(ofMillis(t))),

    /** Commit sync. */
    COMMIT_ASYNC((cnsmr, t) -> {
        CountDownLatch latch = new CountDownLatch(1);

        AtomicReference<RuntimeException> err = new AtomicReference<>();

        cnsmr.commitAsync((m, e) -> {
            if (e != null)
                err.set(new RuntimeException(e));

            latch.countDown();
        });

        boolean isDone;

        try {
            isDone = latch.await(t * 2, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (!isDone && err.get() != null)
            throw err.get();
    }),

    /** Partitions. */
    PARTITIONS((cnsmr, t) -> cnsmr.partitionsFor(TOPIC, ofMillis(t))),

    /** End offsets. */
    END_OFFSETS((cnsmr, t) -> cnsmr.endOffsets(List.of(new TopicPartition(TOPIC, 0)), ofMillis(t)));

    /** Consumer action. */
    private final BiConsumer<KafkaConsumer<?, ?>, Long> action;

    /** */
    ConsumerAction(BiConsumer<KafkaConsumer<?, ?>, Long> action) {
        this.action = action;
    }

    /** */
    public void accept(KafkaConsumer<?, ?> cnsmr, long timeout) {
        action.accept(cnsmr, timeout);
    }
}
