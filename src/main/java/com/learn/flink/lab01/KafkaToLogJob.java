package com.learn.flink.lab01;

import com.learn.flink.common.JsonUtil;
import com.learn.flink.common.LabUtils;
import com.learn.flink.model.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * LAB 1 — The basic end-to-end pipeline:  Kafka -> Flink -> log file.
 *
 * Concepts you can explain after this lab:
 *   - Source   : where data enters the job (KafkaSource)
 *   - Transformation : map() parses JSON into a typed Transaction
 *   - Sink     : where results leave the job (FileSink writing to /tmp)
 *   - Job graph / operator chaining (see the DAG in the Web UI at :8081)
 *
 * Run:
 *   flink run -c com.learn.flink.lab01.KafkaToLogJob target/flink-learning-1.0.jar
 *   tail -f /tmp/flink-output/lab01/&#42;&#47;&#42;
 */
public class KafkaToLogJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // FileSink commits files only on checkpoint, so we must enable checkpointing.
        env.enableCheckpointing(10_000);

        // 1) SOURCE: read raw JSON strings from Kafka
        DataStream<String> raw = env.fromSource(
                LabUtils.kafkaSource("lab01"),
                WatermarkStrategy.noWatermarks(),
                "kafka-source");

        // 2) TRANSFORM: parse JSON -> Transaction -> a human-readable line
        DataStream<String> lines = raw
                .map(JsonUtil::toTransaction).name("parse-json")
                .map(txn -> "user=" + txn.userId
                        + " category=" + txn.category
                        + " amount=" + txn.amount
                        + " eventTime=" + txn.eventTime).name("format-line");

        // 3) SINK: write to a rolling log file AND print to the TaskManager stdout
        lines.sinkTo(LabUtils.fileSink("lab01")).name("file-sink");
        lines.print().name("stdout");

        env.disableOperatorChaining();
        
        // 4) EXECUTE: start the job
        env.execute("Lab 01 - Kafka to Log");
    }
}
