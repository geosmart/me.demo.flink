package me.demo.flink.split

import me.demo.flink.source.ClickSource
import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import java.time.Duration

class SplitStreamTest {


}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 4

    val elementStream = env.addSource(ClickSource())

    //输出标签
    val tagMary: OutputTag<Tuple3<String, String, Long>> = object : OutputTag<Tuple3<String, String, Long>>("mary") {}

    val tagLily: OutputTag<Tuple3<String, String, Long>> = object : OutputTag<Tuple3<String, String, Long>>("lily") {}

    val stream: SingleOutputStreamOperator<Event> = elementStream.assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness<Event>(Duration.ZERO)
            .withTimestampAssigner { element, _ -> element.timestamp }
    )
    // 侧输出流，给不同用户打上不同的标签
    val processStream: SingleOutputStreamOperator<Event> = stream.process(
        object : ProcessFunction<Event, Event>() {
            override fun processElement(value: Event, ctx: Context, out: Collector<Event>) {
                when (value.user) {
                    "mary" -> {
                        ctx.output(tagMary, Tuple3.of(value.user, value.url, value.timestamp))
                    }
                    "lily" -> {
                        ctx.output(tagMary, Tuple3.of(value.user, value.url, value.timestamp))
                    }
                    else -> {
                        out.collect(value)
                    }
                }
            }
        }
    )
    // 根据标签侧输出流：分流可根据需要设置输出类型
    processStream.print("else")
    processStream.getSideOutput(tagMary).print("mary")
    processStream.getSideOutput(tagLily).print("lily")
    env.execute()
}
