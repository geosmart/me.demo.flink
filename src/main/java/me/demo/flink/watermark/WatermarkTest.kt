package me.demo.flink.watermark

import me.demo.flink.source.Event
import org.apache.flink.api.common.eventtime.TimestampAssigner
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier
import org.apache.flink.api.common.eventtime.Watermark
import org.apache.flink.api.common.eventtime.WatermarkGenerator
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier
import org.apache.flink.api.common.eventtime.WatermarkOutput
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Preconditions
import java.time.Duration
import kotlin.math.max

class TransformFilterTest {


}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 4
    // 水位线100ms生成
    env.config.autoWatermarkInterval = 100

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
    // 单调递增的有序流的水位线策略
    elementStream
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forMonotonousTimestamps<Event>()
                .withTimestampAssigner { element, _ -> element.timestamp }
        )


    // 乱序流
    elementStream
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness<Event>(Duration.ofSeconds(2))
                .withTimestampAssigner { element, _ -> element.timestamp }
        )

    env.execute()


    // 自定义水位线：周期性生成水位线
//    val operator: SingleOutputStreamOperator<Event> =
//        elementStream.assignTimestampsAndWatermarks { }


    // 自定义水位线：断点式性生成水位线
//    val operator: SingleOutputStreamOperator<Event> =
//        elementStream.assignTimestampsAndWatermarks { }

    //在自定义数据源中定义水位线


}

//自定义水位线生成器
class CustomBoundedWatermarkGenerator(maxOutOfOrderness: Duration) : WatermarkGenerator<Event> {

    /**  观察到的最大时间  */
    private var maxTimestamp: Long = 0

    /** 数据乱序的延迟时间 */
    private var outOfOrdernessMillis: Long = 0

    init {
        Preconditions.checkNotNull(maxOutOfOrderness, "maxOutOfOrderness")
        Preconditions.checkArgument(!maxOutOfOrderness.isNegative, "maxOutOfOrderness cannot be negative")
        outOfOrdernessMillis = maxOutOfOrderness.toMillis()
        maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1
    }

    override fun onEvent(event: Event?, eventTimestamp: Long, output: WatermarkOutput?) {
        //断点式生成水位线：每来一条数据调用一次，更新当前最大时间戳
        maxTimestamp = max(maxTimestamp, eventTimestamp)
    }

    override fun onPeriodicEmit(output: WatermarkOutput) {
        //周期性生成水位线：发射水位线，默认20ms调用一次，-1是因为是左闭右开[8点,9点)，9点-1ms的数据不包含在水位线内
        output.emitWatermark(Watermark(maxTimestamp - outOfOrdernessMillis - 1))
    }

}

class CustomWatermarkStrategy : WatermarkStrategy<Event> {
    //提取时间戳分配器：从流中数据元素中的某个字段中提取timestamp，并分配给元素
    override fun createTimestampAssigner(context: TimestampAssignerSupplier.Context)
        : TimestampAssigner<Event> {
        // 和withTimestampAssigner效果一样
        return TimestampAssigner { element, _ -> element.timestamp }
    }

    // 水位线生成器：按照既定的方式，基于时间戳生成水位线
    override fun createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context):
        WatermarkGenerator<Event> {
        return CustomBoundedWatermarkGenerator(Duration.ofSeconds(2))

    }
}
