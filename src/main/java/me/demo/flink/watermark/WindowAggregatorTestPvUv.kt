package me.demo.flink.watermark

import me.demo.flink.source.ClickSource
import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

class WindowAggregatorTestPvUv {


}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1

    // 计窗时间窗口内，用户的PV/UV的比值：表示站点的用户粘性
    val stream: SingleOutputStreamOperator<Event> = env.addSource(ClickSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forMonotonousTimestamps<Event>()
                .withTimestampAssigner { element, _ -> element.timestamp }
        )
    stream.print("data")

    val avgOutput: SingleOutputStreamOperator<String> = stream.keyBy { true }
        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
        .aggregate(AvgPVFun())
    avgOutput.print("APT:TumblingEventTimeWindows")
    env.execute()
}


/***
 * IN：Event
 * ACC：Tuple2<访问时间,次数>
 * OUT：平均访问时间
 */
class AvgPVFun : AggregateFunction<Event, Tuple2<Long, HashSet<String>>, String> {
    override fun createAccumulator(): Tuple2<Long, HashSet<String>> {
        return Tuple2.of(0, HashSet())
    }

    override fun add(value: Event, accumulator: Tuple2<Long, HashSet<String>>): Tuple2<Long, HashSet<String>> {
        // 每条数据，pv+1，将user放入hashset
        accumulator.f1.add(value.user)
        return Tuple2.of(accumulator.f0 + 1, accumulator.f1)
    }

    override fun getResult(accumulator: Tuple2<Long, HashSet<String>>): String {
        //窗口触发时，输出pv和uv的比值
        return (accumulator.f0 / accumulator.f1.size.toDouble()).toString()
    }

    override fun merge(
        a: Tuple2<Long, HashSet<String>>,
        b: Tuple2<Long, HashSet<String>>,
    ): Tuple2<Long, HashSet<String>> {
        a.f1.addAll(b.f1)
        return Tuple2.of(a.f0 + b.f0, a.f1)
    }
}
