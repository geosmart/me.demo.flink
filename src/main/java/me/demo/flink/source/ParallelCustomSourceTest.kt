package me.demo.flink.source

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class ParallelCustomSourceTest {
}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 4

    // 从socket读取, nc -lk 9876
    val source: DataStreamSource<Int> = env
        .addSource(ParallelCustomSource())
        .setParallelism(2)

    source.print("source")
    env.execute()

}
