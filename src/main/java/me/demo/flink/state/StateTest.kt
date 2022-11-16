package me.demo.flink.state

import me.demo.flink.source.ClickSource
import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.AggregatingState
import org.apache.flink.api.common.state.AggregatingStateDescriptor
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.state.ReducingState
import org.apache.flink.api.common.state.ReducingStateDescriptor
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

    var valueState: ValueState<Event>? = null
    var listState: ListState<Event>? = null
    var mapState: MapState<String, Long>? = null
    var reducingState: ReducingState<Event>? = null
    var aggState: AggregatingState<Event, String>? = null

    override fun open(parameters: Configuration) {
        super.open(parameters)
        //state初始化
        valueState = runtimeContext.getState(ValueStateDescriptor("value-state", Event::class.java))
        listState = runtimeContext.getListState(ListStateDescriptor("list-state", Event::class.java))
        mapState = runtimeContext.getMapState(MapStateDescriptor("map-state", String::class.java, Long::class.java))
        reducingState = runtimeContext.getReducingState(ReducingStateDescriptor("reducing-state",
            { value1, value2 -> Event(value1.user, value1.url, value2.timestamp) },
            Event::class.java)
        )
        aggState = runtimeContext.getAggregatingState(
            AggregatingStateDescriptor("agg-state",
                object : AggregateFunction<Event, Long, String> {
                    override fun createAccumulator(): Long {
                        return 0L
                    }

                    override fun add(value: Event, accumulator: Long): Long {
                        return accumulator.inc()
                    }

                    override fun getResult(accumulator: Long): String {
                        return "count:$accumulator"
                    }

                    override fun merge(a: Long, b: Long): Long {
                        return a + b
                    }
                }, Long::class.java))
    }


    override fun flatMap(value: Event, out: Collector<String>) {
        //后续每次打印当前key的最新值，key之间状态隔离
        valueState!!.update(value)
        println("${value.user}，valueState :${valueState!!.value()}")

        // listState
        listState!!.add(value)
        println("${value.user}，listState size :${listState!!.get().toList().size}")

        // mapState
        if (mapState!!.get(value.user) != null) {
            mapState!!.put(value.user, mapState!!.get(value.user) + 1)
            println("${value.user}，get mapState:${mapState!!.get(value.user)}")
        }

        // reducing state
        reducingState!!.add(value)
        println("${value.user}，reducingState  :${reducingState!!.get().timestamp}")

        // agg state
        aggState!!.add(value)
        println("${value.user}，aggState  :${aggState!!.get()}")
    }

}


