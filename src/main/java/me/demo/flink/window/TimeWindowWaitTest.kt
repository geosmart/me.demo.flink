package me.demo.flink.window

import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.sql.Timestamp
import java.util.Random

class TimeWindowTest2 {


}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1
    // 水位线100ms生成
    env.config.autoWatermarkInterval = 100

    // 从输入对象读取
    val elementStream: DataStreamSource<Event> = env.addSource(MockNoNewDataClickSource())
    // 单调递增的有序流的水位线策略
    elementStream
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forMonotonousTimestamps<Event>()
                .withTimestampAssigner { element, _ -> element.timestamp }
        )
        .keyBy { true }
        // 如果事件中的水位前不往前推动，那process就不会触发
        // 如果想要在某个事件触发后定时触发，不适合使用窗口，keyby后使用定时器即可
        .window(TumblingEventTimeWindows.of(Time.seconds(2)))
        .process(TestNoDataInWindow())
        .print("time-window")

    env.execute()
}

class MockNoNewDataClickSource : SourceFunction<Event> {

    var running = true

    override fun run(ctx: SourceFunction.SourceContext<Event>) {
        val users = listOf("mary", "alice", "bob", "cary")
        val urls = listOf("./home", "./cart", "./fav", "./prod?id=100", "./prod?id=10")
        val random = Random()

        while (running) {
            //随机模拟数据
            val mockEvent = Event(
                users[random.nextInt(users.size)],
                urls[random.nextInt(urls.size)],
                1000L)
            ctx.collect(mockEvent)
            //println(mockEvent)
            Thread.sleep(1000)
        }
    }

    override fun cancel() {
        running = false
    }


}

class TestNoDataInWindow : ProcessWindowFunction<Event, String, Boolean, TimeWindow>() {
    override fun process(key: Boolean, context: Context, elements: MutableIterable<Event>, out: Collector<String>) {
        val uv = elements.map { it.user }.toSet().size.toString()
        // 结合窗口信息输出
        val start = Timestamp(context.window().start)
        val end = Timestamp(context.window().end)
        out.collect("windows[$start,$end],UV=$uv")
    }
}
