package me.demo.flink.watermark

import me.demo.flink.source.CustomSource
import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.sql.Timestamp

class UvCountExample {
}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1
    // 水位线100ms生成
    env.config.autoWatermarkInterval = 100

    //使用ProcessWindowFunction计算UV
    val uvAgg = env.addSource(CustomSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forMonotonousTimestamps<Event>()
                .withTimestampAssigner { element, _ -> element.timestamp }
        )
    uvAgg.print("input")

    //使用AggregateFunction和ProcessWindowFunction结合计算UV
    uvAgg.keyBy { true }
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .aggregate(UvAggFunction(), UvProcessWindowFunction())
        .print("uvAgg")
    env.execute()
}

/***
 * 自定义AggregationFunction，增量聚合计算UV
 */
class UvAggFunction : AggregateFunction<Event, HashSet<String>, Long> {
    override fun createAccumulator(): HashSet<String> {
        return HashSet()
    }

    override fun add(value: Event, accumulator: HashSet<String>): HashSet<String> {
        accumulator.add(value.user)
        return accumulator
    }

    override fun getResult(accumulator: HashSet<String>): Long {
        return accumulator.size.toLong()
    }

    override fun merge(a: HashSet<String>?, b: HashSet<String>?): HashSet<String>? {
        return null
    }
}

/***
 * 自定义实现ProcessWindowFunction，结合窗口信息输出
 */
class UvProcessWindowFunction : ProcessWindowFunction<Long, String, Boolean, TimeWindow>() {
    override fun process(
        key: Boolean, context: Context,
        elements:
        MutableIterable<Long>,
        out: Collector<String>,
    ) {
        // 结合窗口信息输出
        val start = Timestamp(context.window().start)
        val end = Timestamp(context.window().end)
        val uv = elements.iterator().next().toLong()
        out.collect("windows[$start,$end],UV=$uv")
    }

}

