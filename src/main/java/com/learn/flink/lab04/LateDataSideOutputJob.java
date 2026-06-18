package com.learn.flink.lab04;

import com.learn.flink.common.JsonUtil;
import com.learn.flink.common.LabUtils;
import com.learn.flink.model.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * LAB 4 — Late data handling with allowed lateness + side outputs.
 *
 * Concepts you can explain after this lab:
 *   - "Late" event = arrives after the watermark has already passed its window end
 *   - allowedLateness(): keep windows around a bit longer to still include slightly-late events
 *   - Side output (OutputTag): route data that is STILL too late into a separate stream
 *     instead of silently dropping it (very common in real pipelines for audit/repair)
 *
 * The producer emits ~10% of events 20s in the past, so the "late" file will fill up.
 *
 * Run:
 *   flink run -c com.learn.flink.lab04.LateDataSideOutputJob target/flink-learning-1.0.jar
 *   tail -f /tmp/flink-output/lab04/ontime/&#42;&#47;&#42;
 *   tail -f /tmp/flink-output/lab04/late/&#42;&#47;&#42;
 */
public class LateDataSideOutputJob {

    // OutputTag must be an anonymous subclass so Flink can capture the generic type.
    private static final OutputTag<Transaction> LATE = new OutputTag<Transaction>("late-events") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<Transaction> withTime = env
                .fromSource(LabUtils.kafkaSource("lab04"), WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(JsonUtil::toTransaction).name("parse-json")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((txn, ts) -> txn.eventTime));

        SingleOutputStreamOperator<String> windowed = withTime
                .keyBy(txn -> txn.category)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))   // tolerate events up to 5s late
                .sideOutputLateData(LATE)            // anything still later -> side output
                .process(new CategoryCount()).name("window-with-lateness");

        // main (on-time) results
        windowed.sinkTo(LabUtils.fileSink("lab04/ontime")).name("ontime-sink");

        // late results: recover the dropped events into their own file
        DataStream<String> late = windowed.getSideOutput(LATE)
                .map(txn -> "LATE user=" + txn.userId + " category=" + txn.category
                        + " amount=" + txn.amount + " eventTime=" + txn.eventTime)
                .name("format-late");
        late.sinkTo(LabUtils.fileSink("lab04/late")).name("late-sink");
        late.print().name("late-stdout");

        env.execute("Lab 04 - Late Data & Side Output");
    }

    public static class CategoryCount
            extends ProcessWindowFunction<Transaction, String, String, TimeWindow> {
        @Override
        public void process(String category, Context ctx,
                            Iterable<Transaction> elements, Collector<String> out) {
            long count = 0;
            for (Transaction ignored : elements) {
                count++;
            }
            out.collect("window[" + ctx.window().getStart() + " - " + ctx.window().getEnd() + "] "
                    + "category=" + category + " count=" + count);
        }
    }
}
