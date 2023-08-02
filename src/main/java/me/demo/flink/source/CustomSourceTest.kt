package me.demo.flink.source

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class CustomSourceTes {
}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1


    //  setParallelism只能为1，否则抛出异常：The parallelism of non parallel operator must be 1.
    val source: DataStreamSource<Event> = env.addSource(ClickSource())
        .setParallelism(1)

    source.print("source")
    env.execute()

}
