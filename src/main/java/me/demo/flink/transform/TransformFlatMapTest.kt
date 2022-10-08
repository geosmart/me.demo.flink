package me.demo.flink.transform

import me.demo.flink.source.Event
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

class TransformFlatMapTest {


}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 4

    // 从输入对象读取
    val elementStream: DataStreamSource<Event> = env.fromElements(
        Event("lily", "/home", 1000),
        Event("lucy", "/home", 2000)
    )

    //自定义MapFunction
    val res1: SingleOutputStreamOperator<String> = elementStream.flatMap(EventFlatMapFunction())

    res1.print("class map")

    //自定义MapFunction:4种写法
    var res2: SingleOutputStreamOperator<String> = elementStream.flatMap { value: Event, out ->
        if (value.user == "lily") {
            out.collect(value.user)
        } else if (value.user == "lucy") {
            out.collect(value.url)
            out.collect(value.user)
            out.collect(value.timestamp.toString())
        }
    }
    // 函数-声明类型
    res2 = elementStream.flatMap(
        fun(value: Event, out: Collector<String>) {
            if (value.user == "lily") {
                out.collect(value.user)
            } else if (value.user == "lucy") {
                out.collect(value.url)
                out.collect(value.user)
                out.collect(value.timestamp.toString())
            }
        })

    res2.print("lambda map")
    env.execute()

}

class EventFlatMapFunction : FlatMapFunction<Event, String> {
    override fun flatMap(value: Event, out: Collector<String>) {
        if (value.user == "lily") {
            out.collect(value.user)
        } else if (value.user == "lucy") {
            out.collect(value.url)
            out.collect(value.user)
            out.collect(value.timestamp.toString())
        }
    }

}
