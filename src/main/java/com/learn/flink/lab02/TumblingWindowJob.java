package com.learn.flink.lab02;

import com.learn.flink.common.JsonUtil;
import com.learn.flink.common.LabUtils;
import com.learn.flink.model.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * LAB 2 — Event time, watermarks, and tumbling windows.
 *
 * Concepts you can explain after this lab:
 *   - Event time vs processing time (we use the event's own timestamp)
 *   - Watermarks: how Flink decides "event time has advanced enough to close a window"
 *   - Tumbling window: fixed, non-overlapping 10-second buckets
 *   - Per-key aggregation: one running result per category, per window
 *
 * Run:
 *   flink run -c com.learn.flink.lab02.TumblingWindowJob target/flink-learning-1.0.jar
 *   tail -f /tmp/flink-output/lab02/&#42;&#47;&#42;
 */
public class TumblingWindowJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<Transaction> txns = env
                .fromSource(LabUtils.kafkaSource("lab02"), WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(JsonUtil::toTransaction).name("parse-json");

        // Tell Flink to use each event's own time, allowing up to 5s of out-of-orderness.
        // The watermark = (max event time seen) - 5s. A window closes once the watermark passes its end.
        DataStream<Transaction> withTime = txns.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((txn, ts) -> txn.eventTime));

        DataStream<String> windowed = withTime
                .keyBy(txn -> txn.category)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new CategorySummary()).name("10s-tumbling-window");

        windowed.sinkTo(LabUtils.fileSink("lab02")).name("file-sink");
        windowed.print().name("stdout");

        env.disableOperatorChaining();
        env.execute("Lab 02 - Event Time Tumbling Window");
    }

    /** Summarizes all transactions of one category that fall into one 10s window. */
    public static class CategorySummary
            extends ProcessWindowFunction<Transaction, String, String, TimeWindow> {

        @Override
        public void process(String category, Context ctx,
                            Iterable<Transaction> elements, Collector<String> out) {
            long count = 0;
            double total = 0;
            for (Transaction t : elements) {
                count++;
                total += t.amount;
            }
            out.collect("window[" + ctx.window().getStart() + " - " + ctx.window().getEnd() + "] "
                    + "category=" + category
                    + " count=" + count
                    + " total=" + Math.round(total * 100) / 100.0);
        }
    }
}
