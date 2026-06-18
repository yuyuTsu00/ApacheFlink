package com.learn.flink.lab05;

import com.learn.flink.common.JsonUtil;
import com.learn.flink.common.LabUtils;
import com.learn.flink.model.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * LAB 5 — Low-level control with KeyedProcessFunction + timers.
 *
 * Concepts you can explain after this lab:
 *   - KeyedProcessFunction: full access to state + time + per-key timers
 *   - Registering a processing-time timer and reacting in onTimer()
 *   - A classic pattern: "alert if a user has been INACTIVE for 30 seconds"
 *
 * How it works: every transaction (re)arms a 30s timer for that user. If a new
 * transaction arrives first, we move the timer forward. If 30s pass with no new
 * activity, onTimer() fires and emits an inactivity alert.
 *
 * Run:
 *   flink run -c com.learn.flink.lab05.ProcessFunctionJob target/flink-learning-1.0.jar
 *   tail -f /tmp/flink-output/lab05/&#42;&#47;&#42;
 *   # (tip: stop the producer for ~30s and watch the alerts fire)
 */
public class ProcessFunctionJob {

    private static final long INACTIVITY_MS = 30_000;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<Transaction> txns = env
                .fromSource(LabUtils.kafkaSource("lab05"), WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(JsonUtil::toTransaction).name("parse-json");

        DataStream<String> alerts = txns
                .keyBy(txn -> txn.userId)
                .process(new InactivityDetector()).name("inactivity-detector");

        alerts.sinkTo(LabUtils.fileSink("lab05")).name("file-sink");
        alerts.print().name("stdout");

        env.execute("Lab 05 - ProcessFunction & Timers");
    }

    public static class InactivityDetector extends KeyedProcessFunction<String, Transaction, String> {

        /** stores the timestamp of the currently-registered timer for this user */
        private transient ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) {
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Long.class));
        }

        @Override
        public void processElement(Transaction txn, Context ctx, Collector<String> out) throws Exception {
            // cancel the previous timer (if any) so only the latest activity counts
            Long previous = timerState.value();
            if (previous != null) {
                ctx.timerService().deleteProcessingTimeTimer(previous);
            }
            long next = ctx.timerService().currentProcessingTime() + INACTIVITY_MS;
            ctx.timerService().registerProcessingTimeTimer(next);
            timerState.update(next);

            out.collect("activity user=" + ctx.getCurrentKey() + " amount=" + txn.amount);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
            out.collect("ALERT inactive user=" + ctx.getCurrentKey()
                    + " (no transactions for " + (INACTIVITY_MS / 1000) + "s)");
            timerState.clear();
        }
    }
}
