package com.example;

import java.util.List;
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

    /** Commit. */
    COMMIT((cnsmr, t) -> cnsmr.commitSync(ofMillis(t))),

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
