package me.demo.flink.udf

import me.demo.flink.source.ClickSource
import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.sql.Timestamp
import java.time.Duration

class TopNParallelWindowFunc {

}

fun main() {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 3
    val urlCountStream = env.addSource(ClickSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness<Event>(Duration.ofSeconds(0))
                .withTimestampAssigner { element, _ -> element.timestamp }
        )

        // 每5s计算一次，10s窗口内的url的访问量
        .keyBy { it.url }
        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
        .aggregate(UrlViewCountAggFunc(), UrlViewCountResultProcessFunc())
    urlCountStream.print("UrlViewCount")
    urlCountStream
        .keyBy { it.windowEnd }
        .process(TopNProcessResult(3))
        .print("TopNProcessResult")
    env.execute()
}

class TopNProcessResult(val n: Int = 0) : KeyedProcessFunction<Long, UrlViewCount, String>() {
    //定义状态存储每个窗口内的数据
    var urlViewCountListState: ListState<UrlViewCount>? = null

    override fun open(parameters: Configuration) {
        //从环境中获取状态
        urlViewCountListState = runtimeContext.getListState(
            ListStateDescriptor("url-count-list", Types.POJO(UrlViewCount::class.java))
        )
    }

    override fun processElement(value: UrlViewCount, ctx: Context, out: Collector<String>) {
        //将数据保存到状态中
        urlViewCountListState!!.add(value)

        //注册定时器，1ms后，计算本窗口内的所有数据的TopN
        ctx.timerService().registerProcessingTimeTimer(ctx.currentKey + 1)
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<String>) {
        super.onTimer(timestamp, ctx, out)
        val urlViewCountList = urlViewCountListState!!.get().toList()
        val max = urlViewCountList.size.coerceAtMost(n)
        val topN = urlViewCountList.sortedByDescending { it.count }.subList(0, max)
        val time = "窗口结束时间:${ctx.currentKey}"
        var res = "\n$time\n"
        for ((idx, it) in topN.withIndex()) {
            res += "NO ${idx + 1},url:${it.url},pv:${it.count}\n"
        }
        out.collect(res)
    }
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

