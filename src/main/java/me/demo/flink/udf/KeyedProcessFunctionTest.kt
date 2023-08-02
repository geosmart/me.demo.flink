package me.demo.flink.udf

import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.util.Collector
import java.sql.Timestamp
import java.time.Duration

class ProcessFunctionTest {

}

fun main() {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1
    env.addSource(CustomSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness<Event>(Duration.ofSeconds(0))
                .withTimestampAssigner { element, _ -> element.timestamp }
        ).keyBy { it.user }
        .process(CustomProcessFunction())
        .print("processFunction")
    env.execute()
}

class CustomProcessFunction : KeyedProcessFunction<String, Event, String>() {
    override fun open(parameters: Configuration?) {
        super.open(parameters)
    }

    override fun processElement(value: Event, ctx: Context, out: Collector<String>) {
        val currTs = ctx.timestamp()
        out.collect("${ctx.currentKey}，数据到达" +
            "，时间戳:${Timestamp(currTs)}" +
            ",watermark:${ctx.timerService().currentWatermark()}")

        //注册1个10s后执行的定时器
        ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L)
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<String>) {
        super.onTimer(timestamp, ctx, out)
        out.collect("${ctx.currentKey},定时器触发" +
            ",触发时间:${Timestamp(timestamp)},watermark:${ctx.timerService().currentWatermark()}")
    }
}

class CustomSource : SourceFunction<Event> {
    override fun run(ctx: SourceFunction.SourceContext<Event>) {
        ctx.collect(Event("Mary", "./home", 1000L))
        Thread.sleep(5000L)
        ctx.collect(Event("Alice", "./home", 6000L))
        Thread.sleep(5000L)
        ctx.collect(Event("Bob", "./home", 11000L))
        Thread.sleep(5000L)
        ctx.collect(Event("Bob2", "./home", 14000L))
        ctx.collect(Event("Bob3", "./home", 15001L))
        ctx.collect(Event("Bob4", "./home", 16001L))
    }

    override fun cancel() {
        TODO("Not yet implemented")
    }

}

