package me.demo.flink.transform

import me.demo.flink.source.Event
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class TransformReduceTest {


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
        Event("alice", "/prod?id=100", 2),
        Event("bob", "/prod?id=2", 4),
        Event("bob", "/prod?id=2", 4),
        Event("bob", "/home", 3),
        Event("alice", "/prod?id=100", 2),
        Event("bob", "/prod?id=1", 2),
        Event("bob", "/prod?id=3", 5)
    )
    //按键分组后聚合，获取当前访问量最大的用户


    //按键分组后聚合，统计每个用户的访问量
    val clicksByUser: SingleOutputStreamOperator<Tuple2<String, Long>> = elementStream
        .map { Tuple2(it.user, 1L) }.keyBy { it.f0 }
        .reduce { value1, value2 ->
            Tuple2.of(value1.f0, value1.f1 + value2.f1)
        }
    clicksByUser.print("clicksByUser")

    //获取当前最活跃的用户
    val res=clicksByUser.keyBy { "key" }
        .reduce { value1, value2 ->
            if (value1.f1 >= value2.f1) {
                value1
            } else {
                value2
            }
        }

    res.print("result")

    env.execute()
}
