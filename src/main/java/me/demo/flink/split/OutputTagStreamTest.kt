package me.demo.flink.split

import me.demo.flink.source.ClickSource
import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import java.time.Duration

/***
 * 侧输出流-批流一体示例
 */
fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1

    val elementStream = env.addSource(ClickSource())

    //输出标签
    val tagUnMatch = object : OutputTag<Tuple2<String, Event>>("un-match") {}

    val stream = elementStream.assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness<Event>(Duration.ofSeconds(2))
            .withTimestampAssigner { element, _ -> element.timestamp }
    )
    // 侧输出流，给不同用户打上不同的标签
    val processStream = stream.process(
        object : ProcessFunction<Event, Event>() {
            override fun processElement(value: Event, ctx: Context, out: Collector<Event>) {
                if (isMatch(value)) {
                    // 打标后，侧输出流处理
                    out.collect(value)
                } else {
                    // 正常业务
                    ctx.output(tagUnMatch, Tuple2.of(value.user, value))
                }
            }
        }
    )
    // 正常写入kafka
    processStream.print("match")

    // 打标后，延迟写入kafka
    processStream.getSideOutput(tagUnMatch)
        .process(object : ProcessFunction<Tuple2<String, Event>, Event>() {
            override fun processElement(value: Tuple2<String, Event>, ctx: Context, out: Collector<Event>) {
                // 延迟写入kafka
                ctx.output(tagUnMatch, value)
            }
        })
    env.execute()
}

fun isMatch(value: Event): Boolean {
    // todo 打标逻辑
    return value.user == "lily"
}
