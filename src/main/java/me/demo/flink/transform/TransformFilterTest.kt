package me.demo.flink.transform

import me.demo.flink.source.Event
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class TransformFilterTest {


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
    val res1: SingleOutputStreamOperator<Event> = elementStream.filter(EventFilterFunction())

    res1.print("class map")

    //自定义MapFunction:4种写法
    var res2: SingleOutputStreamOperator<Event> = elementStream.filter { it.user == "lily" }
    // 匿名class
    res2 = elementStream.filter(
        fun(it: Event): Boolean {
            return it.user == "lily"
        })

    res2.print("lambda filter")
    env.execute()

}

class EventFilterFunction : FilterFunction<Event> {
    override fun filter(value: Event): Boolean {
        return value.user == "lily"
    }
}
