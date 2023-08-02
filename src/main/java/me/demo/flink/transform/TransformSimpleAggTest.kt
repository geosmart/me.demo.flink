package me.demo.flink.transform

import me.demo.flink.source.Event
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

class TransformSimpleAggTest {


}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1

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
    //按键分组后聚合，获取当前用户最近一次访问的数据
    val res:SingleOutputStreamOperator<Event> = elementStream
        .keyBy { it.user }
        .maxBy("timestamp")

    res.print("max")

    env.execute()
}
