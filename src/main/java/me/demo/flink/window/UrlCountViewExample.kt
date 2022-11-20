package me.demo.flink.window

import me.demo.flink.source.ClickSource
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
import java.time.Duration

class UrlCountViewExample {
}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1
    // 水位线100ms生成
    env.config.autoWatermarkInterval = 100

    //使用ProcessWindowFunction计算UV
    val uvAgg = env.addSource(ClickSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness<Event>(Duration.ofSeconds(3))
                .withTimestampAssigner { element, _ -> element.timestamp }
        )
    uvAgg.print("input")

    //按窗口统计每个url的访问量
    uvAgg.keyBy { it.url }
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .aggregate(UrlViewCountAggFunc(), UrlViewCountResultProcessFunc())
        .print("uvAgg")
    env.execute()
}

class UrlViewCount {
    var url: String = ""
    var count: Long = 0
    var windowStart: Long = 0
    var windowEnd: Long = 0
    override fun toString(): String {
        return "windows[${Timestamp(windowStart)},${Timestamp(windowEnd)}],url=$url,count=$count"
    }
}

/***
 * 增量聚合计算页面访问数
 */
class UrlViewCountAggFunc : AggregateFunction<Event, Long, Long> {
    override fun createAccumulator(): Long {
        return 0
    }

    override fun add(value: Event, accumulator: Long): Long {
        return accumulator + 1
    }

    override fun getResult(accumulator: Long): Long {
        return accumulator
    }

    override fun merge(a: Long?, b: Long?): Long? {
        return null
    }

}

/***
 * ，结合窗口信息输出，输出UrlViewCount
 */
class UrlViewCountResultProcessFunc : ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>() {
    override fun process(
        key: String,
        context: Context, elements: MutableIterable<Long>,
        out: Collector<UrlViewCount>,
    ) {
        // 结合窗口信息输出
        val res = UrlViewCount().apply {
            windowStart = context.window().start
            windowEnd = context.window().end
            count = elements.iterator().next()
            url = key
        }
        out.collect(res)
    }

}

