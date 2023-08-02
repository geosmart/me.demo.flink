package me.demo.flink.transform

import me.demo.flink.source.Event
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class TransformMapTest {


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
    val res1: SingleOutputStreamOperator<String> = elementStream.map(EventMapFunction())

    res1.print("class map")

    //自定义MapFunction:4种写法
    var res2: SingleOutputStreamOperator<String> = elementStream.map { it.user }
    // 函数-声明类型
    res2 = elementStream.map(
        fun(it: Event): String {
            return it.user
        })

    res2.print("lambda map")
    env.execute()

}

class EventMapFunction : MapFunction<Event, String> {
    override fun map(value: Event): String {
        return value.user
    }
}
