package me.demo.flink.udf

import me.demo.flink.source.Event
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class TransformUdfTest {

}

fun main() {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()

    // 从输入对象读取
    val elementStream: DataStreamSource<Event> = env.fromElements(
        Event("bob", "/cart", 1),
        Event("alice", "/prod?id=100", 3),
        Event("alice", "/prod?id=100", 2),
        Event("bob", "/prod?id=2", 4),
        Event("bob", "/home", 3),
        Event("bob", "/prod?id=1", 2),
        Event("bob", "/prod?id=3", 5)
    )
    val res = elementStream.filter(KeywordFilter("prod"))
    res.print("prod")

    env.execute()
}


class KeywordFilter(_keyword: String) : FilterFunction<Event> {
    var keyword: String = _keyword

    override fun filter(value: Event): Boolean {
        return value.url.contains(this.keyword)
    }

}
