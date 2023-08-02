package me.demo.flink.stream

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import java.time.Duration

class WindowJoinTest {


}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1

    val stream1: SingleOutputStreamOperator<Tuple2<String, Long>> = env.fromElements(
        Tuple2.of("a", 1000L),
        Tuple2.of("b", 1000L),
        Tuple2.of("a", 2000L),
        Tuple2.of("b", 2000L),
        Tuple2.of("b", 5100L)
    ).assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness<Tuple2<String, Long>>(Duration.ofSeconds(0))
            .withTimestampAssigner { element, _ -> element.f1 }
    )

    val stream2: SingleOutputStreamOperator<Tuple2<String, Long>> = env.fromElements(
        Tuple2.of("a", 3000L),
        Tuple2.of("b", 4000L),
        Tuple2.of("a", 4500L),
        Tuple2.of("b", 5500L)
    ).assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness<Tuple2<String, Long>>(Duration.ofSeconds(0))
            .withTimestampAssigner { element, _ -> element.f1 }
    )

    stream1.join(stream2)
        .where { it.f0 }
        .equalTo { it.f0 }
        .window((TumblingEventTimeWindows.of(Time.seconds(5))))
        .apply(MyJoinFunc())
        .print("join测试")

    env.execute()
}

class MyJoinFunc : JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String> {
    override fun join(first: Tuple2<String, Long>, second: Tuple2<String, Long>): String {
        return "$first->$second"
        // if (first.f0 == second.f0) {}
    }
}
