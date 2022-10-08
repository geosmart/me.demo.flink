package me.demo.flink.source

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class SourceTest {
}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1

    // 从文件中读取数据
    val fileStream: DataStreamSource<String> = env.readTextFile("data/input/clicks.csv",
        Charsets.UTF_8.name())

    // 从集合读取
    val collectStream = env.fromCollection(listOf(
        Event("lily", "/home", 1000),
        Event("lucy", "/home", 2000)))

    // 从输入对象读取
    val elementStream = env.fromElements(
        Event("lily", "/home", 1000),
        Event("lucy", "/home", 2000)
    )

    // 从socket读取, nc -lk 9876
    val socketStream = env.socketTextStream("localhost", 9876)

    fileStream.print("fileStream")
    collectStream.print("collectStream")
    elementStream.print("elementStream")
    socketStream.print("socketStream")

    env.execute()

}
