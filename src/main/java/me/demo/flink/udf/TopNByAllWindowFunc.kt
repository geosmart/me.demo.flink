package me.demo.flink.udf

import me.demo.flink.source.ClickSource
import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Duration

class TopNByAllWindowFunc {

}

fun main() {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1
    env.addSource(ClickSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness<Event>(Duration.ofSeconds(0))
                .withTimestampAssigner { element, _ -> element.timestamp }
        )
        // 每5s计算一次，10s内访问量前3的url
        .keyBy { it.url }
        .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
        .aggregate(UrlHashMapCountAgg(), UrlAllWindowResult())
        .print("TopNByAllWindowFunc")

    env.execute()
}

class UrlHashMapCountAgg : AggregateFunction<Event, MutableMap<String, Long>, List<Tuple2<String, Long>>> {
    override fun createAccumulator(): MutableMap<String, Long> {
        return mutableMapOf()
    }

    override fun add(p0: Event, p1: MutableMap<String, Long>): MutableMap<String, Long> {
        if (p0.url in p1.keys) {
            p1[p0.url] = p1[p0.url]!! + 1
        } else {
            p1[p0.url] = 1
        }
        return p1
    }

    override fun getResult(p0: MutableMap<String, Long>): List<Tuple2<String, Long>> {
        return p0.map { Tuple2.of(it.key, it.value) }
            .sortedByDescending { it.f1 }.toList()
    }

    override fun merge(p0: MutableMap<String, Long>?, p1: MutableMap<String, Long>?): MutableMap<String, Long> {
        TODO("Not yet implemented")
    }
}

// 全窗口实现
class UrlAllWindowResult : ProcessAllWindowFunction<List<Tuple2<String, Long>>, String, TimeWindow>() {
    override fun process(context: Context, elements: MutableIterable<List<Tuple2<String, Long>>>, out: Collector<String>) {
        val list = elements.iterator().next()
        val time = "窗口结束时间:${context.window().end}"
        var res = "\n$time\n"
        for ((idx, it) in list.withIndex()) {
            res += "NO ${idx + 1},url:${it.f0}=${it.f1}\n"
            if (idx == 1) {
                break
            }
        }
        out.collect(res)
    }
}
