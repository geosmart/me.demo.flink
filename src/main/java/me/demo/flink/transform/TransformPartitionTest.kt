package me.demo.flink.transform

import me.demo.flink.source.Event
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1

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

    // 随机分区
    elementStream.shuffle().print("shuffle").setParallelism(4)

    // 轮询分区
    elementStream.rebalance().print("rebalance").setParallelism(4)

    /***
    Sets the partitioning of the DataStream so that the output elements are distributed evenly to
    a subset of instances of the next operation in a round-robin fashion.
    The subset of downstream operations to which the upstream operation sends elements depends on\
    he degree of parallelism of both the upstream and downstream operation.

    For example, if the upstream operation has parallelism 2 and the downstream operation has parallelism 4,
    then one upstream operation would distribute elements to two downstream operations
    while the other upstream operation would distribute to the other two downstream operations.

    If, on the other hand, the downstream operation has parallelism 2 while the upstream operation has parallelism 4
    then two upstream operations will distribute to one downstream operation while the other two upstream operations will distribute to the other downstream operations.

    In cases where the different parallelisms are not multiples of each other one or several downstream operations will have a differing number of inputs from upstream operations.
    Returns:
    The DataStream with rescale partitioning set.
     */
    // rescale分区，按上下游的partition个数分组轮训分区
    env.addSource(RescaleSource()).setParallelism(2)
        .rescale()
        .print("rescale").setParallelism(4)

    // 广播分区：每条数据都分配到下来有的所有分区
    elementStream.broadcast().print("broadcast").setParallelism(2)

    // 全局分区：所有数据合并到1个分区,setParallelism无效
    elementStream.global().print("global").setParallelism(2)

    // 自定义分区
    env.fromElements(1, 2, 3, 4, 5, 6, 7, 8).setParallelism(1)
        .partitionCustom(
            { key, _ -> key % 2 },
            { value -> value }
        ).print("partitionCustom").setParallelism(4)

    env.execute()
}


class RescaleSource : RichParallelSourceFunction<Int>() {

    override fun run(ctx: SourceFunction.SourceContext<Int>) {
        for (i in 1..8) {
            // 2,4,6,8    发送到taskmanager0，对应slot 1，2
            // 1,3,5,7    发送到taskmanager1，对应slot 3，4
            // index: 0,1
            if (i % 2 == runtimeContext.indexOfThisSubtask) {
                println("${runtimeContext.indexOfThisSubtask} collect $i")
                ctx.collect(i)
            }
        }
    }

    override fun cancel() {
        println("cancel")
    }

}
