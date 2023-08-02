package me.demo.flink.window

import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.sql.Timestamp

class TimeWindowTest {


}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1
    // 水位线100ms生成
    env.config.autoWatermarkInterval = 100

    // 从输入对象读取
    val elementStream: DataStreamSource<Event> = env.fromElements(
        Event("a", "/cart", 1000),
        Event("b", "/cart", 2000),

        Event("c", "/cart", 5000),
        Event("d", "/home", 6000),

        Event("e", "/home", 11000)
    )
    // 单调递增的有序流的水位线策略
    elementStream
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forMonotonousTimestamps<Event>()
                .withTimestampAssigner { element, _ -> element.timestamp }
        )
        .keyBy { true }
        .window(TumblingEventTimeWindows.of(Time.seconds(20)))
        .process(CountByTimeWindow())
        .print("time-window")

    env.execute()
}

class CountByTimeWindow : ProcessWindowFunction<Event, String, Boolean, TimeWindow>() {
    override fun process(key: Boolean, context: Context, elements: MutableIterable<Event>, out: Collector<String>) {
        val uv = elements.map { it.user }.toSet().size.toString()
        // 结合窗口信息输出
        val start = Timestamp(context.window().start)
        val end = Timestamp(context.window().end)
        out.collect("windows[$start,$end],UV=$uv")
    }
}
