package com.learn.flink.common;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * Shared boilerplate so each lab can focus on the ONE feature it teaches.
 */
public final class LabUtils {

    public static final String BROKERS = "localhost:9092";
    public static final String TOPIC = "transactions";
    public static final String OUTPUT_ROOT = "/tmp/flink-output";

    private LabUtils() {
    }

    /**
     * A Kafka source that reads the "transactions" topic as raw JSON strings.
     * Starting from the earliest offset means every run replays all data (great for learning).
     */
    public static KafkaSource<String> kafkaSource(String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(BROKERS)
                .setTopics(TOPIC)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    /**
     * A FileSink that writes text lines into a directory under /tmp/flink-output.
     *
     * IMPORTANT learning point: FileSink only "commits" finished part-files on a
     * successful checkpoint. That is why every lab enables checkpointing — otherwise
     * you would only ever see ".inprogress" files and think nothing is being written.
     */
    public static FileSink<String> fileSink(String subDir) {
        return FileSink.<String>forRowFormat(
                        new Path(OUTPUT_ROOT + "/" + subDir),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofSeconds(10))
                        .withInactivityInterval(Duration.ofSeconds(10))
                        .withMaxPartSize(MemorySize.ofMebiBytes(64))
                        .build())
                .build();
    }
}
