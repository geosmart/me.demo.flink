package me.demo.flink.state

import me.demo.flink.source.ClickSource
import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import java.time.Duration

class BillCheckExample {


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

    stream.keyBy { it.user }
        .flatMap(MyFlatMap())
        .print()
    env.execute()
}

class MyFlatMap : RichFlatMapFunction<Event, String>() {

    var state: ValueState<Event>? = null

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        val stateDesc = ValueStateDescriptor<Event>("state", Event::class.java)
        state = runtimeContext.getState(stateDesc)
    }

    override fun flatMap(value: Event?, out: Collector<String>?) {
        //第一次为null
        println(state!!.value())
        //后续每次打印当前key的最新值，key之间状态隔离
        state!!.update(value)
        println(state!!.value())
    }

}



