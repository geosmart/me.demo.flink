package me.demo.flink.split

import cn.hutool.core.date.DateUtil
import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import java.sql.Date
import java.time.Duration

/***
可基于outputTag实现lanbda架构，批流一体
---
# Flink 侧输出流的 2 个作用
1. 分隔过滤：充当 filter 算子功能，将源中的不同类型的数据做分割处理。因为使用 filter 算子对数据源进行筛选分割的话，会造成数据流的多次复制，导致不必要的性能浪费，过滤后不需要的数据可以重新写入 Pulsar 或 Kafka 的 topic 中供其他应用消费处理。
2. 延时数据处理：在做对延时迟窗口计算时，对延时迟到的数据进行处理，即时数据迟到也不会造成丢失。

输入：
Alice , /home , 1000
Bob , /cart , 2000
Cary , /prod?id=100 , 3500
Mary , /prod?id=200 , 10000
Mary , /prod?id=200 , 11999

# 触发0-10s窗口计算
Mary , /prod?id=200 , 12000

# 0-10s迟到的数据，实时累加
Mary , /prod?id=200 , 6500
Cary , /prod?id=200 , 8000
Cary , /prod?id=100 , 70000

# 触发10-20s的窗口计算，0-10s窗口彻底关闭
Cary , /prod?id=200 , 72000

# 0-10s的迟到数据，写入侧输出流
Cary , /prod?id=100 , 3500
Cary , /prod?id=100 , 4000


 输出：
source> Event(user='Alice', url='/home', timestamp=1000，1970-01-01 08:00:01.0)
source> Event(user='Bob', url='/cart', timestamp=2000，1970-01-01 08:00:02.0)
source> Event(user='Cary', url='/prod?id=100', timestamp=3500，1970-01-01 08:00:03.5)
source> Event(user='Mary', url='/prod?id=200', timestamp=10000，1970-01-01 08:00:10.0)
source> Event(user='Mary', url='/prod?id=200', timestamp=11999，1970-01-01 08:00:11.999)
source> Event(user='Mary', url='/prod?id=200', timestamp=12000，1970-01-01 08:00:12.0)
result> windows[08:00:00---08:00:10],url=/home,count=1
result> windows[08:00:00---08:00:10],url=/prod?id=100,count=1
result> windows[08:00:00---08:00:10],url=/cart,count=1
source> Event(user='Mary', url='/prod?id=200', timestamp=6500，1970-01-01 08:00:06.5)
source> Event(user='Cary', url='/prod?id=200', timestamp=8000，1970-01-01 08:00:08.0)
result> windows[08:00:00---08:00:10],url=/prod?id=200,count=1
result> windows[08:00:00---08:00:10],url=/prod?id=200,count=2
source> Event(user='Cary', url='/prod?id=100', timestamp=70000，1970-01-01 08:01:10.0)
result> windows[08:00:10---08:00:20],url=/prod?id=200,count=3
source> Event(user='Cary', url='/prod?id=200', timestamp=72000，1970-01-01 08:01:12.0)
source> Event(user='Cary', url='/prod?id=100', timestamp=3500，1970-01-01 08:00:03.5)
late> Event(user='Cary', url='/prod?id=100', timestamp=3500，1970-01-01 08:00:03.5)
source> Event(user='Cary', url='/prod?id=100', timestamp=4000，1970-01-01 08:00:04.0)
late> Event(user='Cary', url='/prod?id=100', timestamp=4000，1970-01-01 08:00:04.0)

 */
fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1
    // 使用nc -lk 12345模拟输入流
    val sourceStream = env.socketTextStream("localhost", 12345)
        .map { it.split(",") }
        .filter { it.size == 3 }
        .map { arr ->
            Event(arr[0].trim(), arr[1].trim(), arr[2].trim().toLong())
        }
    sourceStream.print("source")

    //乱序流的Watermark生成：2s延迟
    val streamOperator = sourceStream.assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness<Event>(
            Duration.ofSeconds(2)
        ).withTimestampAssigner { element, _ -> element.timestamp })

    //输出标签
    val tagLate: OutputTag<Event?> = object : OutputTag<Event?>("late") {}

    //统计每个url的访问量：10滚动窗口，数据允许1m迟到
    val result = streamOperator.keyBy { it!!.url } //滚动事件时间窗口
        .window(TumblingEventTimeWindows.of(Time.seconds(10))) //允许窗口处理迟到数据 允许1分钟的延迟
        .allowedLateness(Time.minutes(1)) //将最后的迟到数据输出到侧输出流
        .sideOutputLateData(tagLate)
        .aggregate(UrlViewCountAgg(), UrlViewCountResultProcessFunc())
    result.print("result")

    // 给晚到的数据打上标签，然后提取出来和所对应的窗口合并
    result.getSideOutput(tagLate).print("late")
    env.execute()
}

/**
 * 自定义实现AggregateFunction, 增量计算url页面的访问量，来一条数据就 +1
 */
class UrlViewCountAgg : AggregateFunction<Event, Long, Long> {
    override fun createAccumulator(): Long {
        return 0L
    }

    override fun add(value: Event?, accumulator: Long?): Long {
        return accumulator!! + 1
    }

    override fun getResult(accumulator: Long?): Long? {
        return accumulator
    }

    override fun merge(a: Long?, b: Long?): Long? {
        return null
    }
}


/**
 * 自定义实现ProcessWindowFunction, 包装窗口信息输出
 */
class UrlViewCountResultProcessFunc : ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>() {
    @Throws(Exception::class)
    override fun process(url: String, context: Context, iterable: MutableIterable<Long>, out: Collector<UrlViewCount>) {
        val urlCount = iterable.iterator().next()
        //集合窗口信息输出
        val start: Long = context.window()!!.start
        val end: Long = context.window()!!.end
        val urlCountView = UrlViewCount().apply {
            this.url = url
            this.count = urlCount
            this.windowStart = start
            this.windowEnd = end
        }
        out.collect(urlCountView)
    }
}


class UrlViewCount {
    var url: String = ""
    var count: Long = 0
    var windowStart: Long = 0
    var windowEnd: Long = 0
    override fun toString(): String {
        val s = DateUtil.format(Date(windowStart), "HH:mm:ss")
        val e = DateUtil.format(Date(windowEnd), "HH:mm:ss")
        return "windows[${s}---${e}],url=$url,count=$count"
    }
}
