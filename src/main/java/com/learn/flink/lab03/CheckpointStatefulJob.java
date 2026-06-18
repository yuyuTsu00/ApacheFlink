package com.learn.flink.lab03;

import com.learn.flink.common.JsonUtil;
import com.learn.flink.common.LabUtils;
import com.learn.flink.model.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * LAB 3 — Keyed state + checkpointing + recovery (the headline interview demo).
 *
 * Concepts you can explain after this lab:
 *   - Keyed state (ValueState): a per-user running count & total that Flink manages for you
 *   - Checkpointing: Flink periodically snapshots all state to /tmp/flink-checkpoints
 *   - Savepoint: a manual, named snapshot you take before stopping a job
 *   - Recovery: restart the job from a savepoint and watch the totals CONTINUE, not reset
 *
 * Demo flow:
 *   flink run -c com.learn.flink.lab03.CheckpointStatefulJob target/flink-learning-1.0.jar
 *   # note the JOB_ID printed (also visible in the Web UI), let totals grow, then:
 *   flink savepoint &lt;JOB_ID&gt; file:///tmp/flink-savepoints
 *   flink cancel &lt;JOB_ID&gt;
 *   flink run -s file:///tmp/flink-savepoints/savepoint-xxxxx \
 *             -c com.learn.flink.lab03.CheckpointStatefulJob target/flink-learning-1.0.jar
 *   # the per-user totals resume where they left off  =&gt;  state survived the restart
 */
public class CheckpointStatefulJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Snapshot all state every 10 seconds, with exactly-once guarantees.
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);

        DataStream<Transaction> txns = env
                .fromSource(LabUtils.kafkaSource("lab03"), WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(JsonUtil::toTransaction).name("parse-json");

        DataStream<String> running = txns
                .keyBy(txn -> txn.userId)
                .process(new RunningTotal()).name("stateful-running-total");

        running.sinkTo(LabUtils.fileSink("lab03")).name("file-sink");
        running.print().name("stdout");

        env.execute("Lab 03 - Checkpointing & Keyed State");
    }

    /**
     * Maintains a per-user running count and total amount in managed keyed state.
     * Because this state is checkpointed, it is restored on recovery / savepoint restart.
     */
    public static class RunningTotal extends KeyedProcessFunction<String, Transaction, String> {

        private transient ValueState<Long> count;
        private transient ValueState<Double> total;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
            total = getRuntimeContext().getState(new ValueStateDescriptor<>("total", Double.class));
        }

        @Override
        public void processElement(Transaction txn, Context ctx, Collector<String> out) throws Exception {
            long c = (count.value() == null ? 0L : count.value()) + 1;
            double t = (total.value() == null ? 0.0 : total.value()) + txn.amount;
            count.update(c);
            total.update(t);

            out.collect("user=" + txn.userId
                    + " runningCount=" + c
                    + " runningTotal=" + Math.round(t * 100) / 100.0);
        }
    }
}
