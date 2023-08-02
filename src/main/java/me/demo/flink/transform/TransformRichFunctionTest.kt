package me.demo.flink.transform

import me.demo.flink.source.Event
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 2

    // 从输入对象读取
    val elementStream: DataStreamSource<Event> = env.fromElements(
        Event("bob", "/cart", 1),
        Event("alice", "/prod?id=100", 2),
        Event("bob", "/prod?id=3", 5)
    )
    //按键分组后聚合，获取当前用户最近一次访问的数据
    val res: SingleOutputStreamOperator<Int> = elementStream.map(MyRichMapper())

    res.print("max")

    env.execute()
}

class MyRichMapper : RichMapFunction<Event, Int>() {
    override fun open(parameters: Configuration?) {
        super.open(parameters)
        println("lifecycle call open " + runtimeContext.indexOfThisSubtask)
    }

    override fun map(value: Event): Int {
        return value.timestamp.toInt()
    }

    override fun close() {
        super.close()
        println("lifecycle call close " + runtimeContext.indexOfThisSubtask)
    }

}
