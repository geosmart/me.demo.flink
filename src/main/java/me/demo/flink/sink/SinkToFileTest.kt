package me.demo.flink.sink

import me.demo.flink.source.Event
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import java.util.concurrent.TimeUnit

class SinkToFileTest {
}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 4

    // 从输入对象读取
    val elementStream: DataStreamSource<Event> = env.fromElements(
        Event("alice", "/cart", 1),
        Event("alice", "/prod?id=100", 2),
        Event("alice", "/prod?id=100", 3),
        Event("alice", "/prod?id=2", 4),
        Event("bob", "/prod?id=100", 1),
        Event("bob", "/prod?id=2", 2),
        Event("bob", "/prod?id=2", 3),
        Event("bob", "/home", 4)
    )
    val sinkFilePath = Path("data/output/fileSink1")

    val fileRollingPolicy: DefaultRollingPolicy<String, String> =
        DefaultRollingPolicy.builder()
            .withMaxPartSize(1024 * 1024 * 100)
            .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
            .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
            .build()

    // StreamingFileSink实现了CheckpointedFunction, CheckpointListener {
    val fileSink = StreamingFileSink.forRowFormat<String>(sinkFilePath, SimpleStringEncoder())
        .withRollingPolicy(fileRollingPolicy).build()

    elementStream.map { it.toString() }.addSink(fileSink)

    env.execute()
}
