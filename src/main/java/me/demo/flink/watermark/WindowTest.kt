package me.demo.flink.watermark

import me.demo.flink.source.CustomSource
import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.sql.Timestamp

class WindowTest {


}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1
    // 水位线100ms生成
    env.config.autoWatermarkInterval = 100

    // 从输入对象读取
    val elementStream: DataStreamSource<Event> = env.fromElements(
        Event("alice", "/cart", 1),
        Event("alice", "/prod?id=100", 2),
        Event("alice", "/prod?id=100", 3),
        Event("alice", "/prod?id=2", 4),
        Event("alice", "/prod?id=100", 3),
        Event("alice", "/prod?id=2", 4),
        Event("bob", "/prod?id=100", 1),
        Event("bob", "/home", 4)
    )
    // 单调递增的有序流的水位线策略
    elementStream
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forMonotonousTimestamps<Event>()
                .withTimestampAssigner { element, _ -> element.timestamp }
        )

    /** TimeWindows */
    // 滚动时间窗口
    elementStream
        .keyBy { it.user }
        .window(TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(15)))

    // 滑动时间窗口
    elementStream
        .keyBy { it.user }
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15)))

    // 会话时间窗口
    elementStream
        .keyBy { it.user }
        .window(EventTimeSessionWindows.withGap(Time.seconds(2)))

    // 会话时间窗口
    elementStream
        .keyBy { it.user }
        .window(EventTimeSessionWindows.withGap(Time.seconds(2)))

    /** CountWindows，基于GlobalWindows */
    // 滚动时间窗口
    elementStream
        .keyBy { it.user }
        .countWindow(3)

    // 计窗时间窗口内用户的访问次数：reduce
    val output: SingleOutputStreamOperator<Tuple2<String, Int>> =
        env.addSource(CustomSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.forMonotonousTimestamps<Event>()
                    .withTimestampAssigner { element, _ -> element.timestamp }
            )
            .map { Tuple2.of(it.user, 1) }
            .keyBy { it.f0 }
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce { v1, v2 -> Tuple2.of(v1.f0, v1.f1 + v2.f1) }
    //output.print("PV：TumblingEventTimeWindows")

    // 计窗时间窗口内用户的平均访问时长：reduce
    val avgOutput: SingleOutputStreamOperator<String> =
        env.addSource(CustomSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.forMonotonousTimestamps<Event>()
                    .withTimestampAssigner { element, _ -> element.timestamp }
            )
            .keyBy { it.user }
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(MyAggFun())
//    avgOutput.print("APT:TumblingEventTimeWindows")

    //使用ProcessWindowFunction计算UV
    val uvProcessWindowOut = env.addSource(CustomSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forMonotonousTimestamps<Event>()
                .withTimestampAssigner { element, _ -> element.timestamp }
        )
//    uvProcessWindowOut.print("input")
    uvProcessWindowOut.keyBy { true}
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .process(UvCountByWindow())
        .print("UvCountByWindow")

    env.execute()
}

/***
<IN> – Event
<OUT> – String
<KEY> – Boolean
<W> – TimeWindow
 */
class UvCountByWindow : ProcessWindowFunction<Event, String, Boolean, TimeWindow>() {
    override fun process(key: Boolean, context: Context, elements: MutableIterable<Event>, out: Collector<String>) {
        val uv = elements.map { it.user }.toSet().size.toString()
        // 结合窗口信息输出
        val start = Timestamp(context.window().start)
        val end = Timestamp(context.window().end)
        out.collect("windows[$start,$end],UV=$uv")
    }
}

/***
 * IN：Event
 * ACC：Tuple2<访问时间,次数>
 * OUT：平均访问时间
 */
class MyAggFun : AggregateFunction<Event, Tuple2<Long, Int>, String> {
    override fun createAccumulator(): Tuple2<Long, Int> {
        return Tuple2.of(0, 0)
    }

    override fun add(value: Event, accumulator: Tuple2<Long, Int>): Tuple2<Long, Int> {
        return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1)
    }

    override fun getResult(accumulator: Tuple2<Long, Int>): String {
        return Timestamp(accumulator.f0 / accumulator.f1).toString()
    }

    override fun merge(a: Tuple2<Long, Int>, b: Tuple2<Long, Int>): Tuple2<Long, Int> {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1)
    }

}


