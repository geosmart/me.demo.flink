package me.demo.flink.state

import me.demo.flink.source.ClickSource
import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import java.time.Duration

class PeriodicPvStateTest {


}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 4

    val stream: SingleOutputStreamOperator<Event> = env
        .addSource(ClickSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness<Event>(Duration.ofSeconds(0))
                .withTimestampAssigner { element, _ -> element.timestamp }
        )
    stream.print("data")
    stream.keyBy { it.user }
        .process(PeriodicResult())
        .print()
    env.execute()
}

class PeriodicResult : KeyedProcessFunction<String, Event, String>() {
    var uvState: ValueState<Long>? = null
    var timerState: ValueState<Long>? = null

    override fun open(parameters: Configuration) {
        super.open(parameters)
        uvState = runtimeContext.getState(ValueStateDescriptor("pv-state", Long::class.java))
        timerState = runtimeContext.getState(ValueStateDescriptor("timer-state", Long::class.java))
    }

    override fun processElement(value: Event, ctx: Context, out: Collector<String>) {
        if (uvState!!.value() == null) {
            uvState!!.update(1)
        } else {
            uvState!!.update(uvState!!.value() + 1)
        }
        //如果没有注册的话，注册定时器，有数据才会触发
        if (timerState!!.value() == null) {
            ctx.timerService().registerProcessingTimeTimer(value.timestamp + 10000)
            timerState!!.update(value.timestamp + 10000)
        }
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<String>) {
        super.onTimer(timestamp, ctx, out)
        //定时器触发，输出一次统计结果
        out.collect("${timestamp}，${ctx.currentKey},pv=${uvState!!.value()}")
        timerState!!.clear()

        //触发后，立即注册下一个10s的定时器
        ctx.timerService().registerProcessingTimeTimer(timestamp + 10000)
        timerState!!.update(timestamp + 10000)
    }
}


