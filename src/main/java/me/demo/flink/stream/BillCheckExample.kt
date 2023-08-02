package me.demo.flink.stream

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import java.time.Duration

class BillCheckExample {


}

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 4


    // 商家平台的订单日志
    val appStream: SingleOutputStreamOperator<Tuple3<String, String, Long>> = env.fromElements(
        Tuple3.of("order-1", "app", 1000L),
        Tuple3.of("order-1", "app", 2000L),
        Tuple3.of("order-3", "app", 3500L)
    ).assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness<Tuple3<String, String, Long>>(Duration.ofSeconds(0))
            .withTimestampAssigner { element, _ -> element.f2 }
    )

    //支付平台的支付日志
    val thirdPartStream: SingleOutputStreamOperator<Tuple4<String, String, String, Long>> = env.fromElements(
        Tuple4.of("order-1", "app", "success", 1000L),
        Tuple4.of("order-1", "app", "success", 2000L),
    ).assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness<Tuple4<String, String, String, Long>>(Duration.ofSeconds(3))
            .withTimestampAssigner { element, _ -> element.f3 }
    )

    //检测在2条流中是否匹配，5s内对账不匹配则告警
    appStream.connect(thirdPartStream)
        .keyBy(
            { value -> value.f0 },
            { value -> value.f0 },
        ).process(BillCheckCoProcessFunc())
        .print("实时对账")
    env.execute()
}

class BillCheckCoProcessFunc : CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>() {
    var appEventState: ValueState<Tuple3<String, String, Long>>? = null
    var thirdPartEventState: ValueState<Tuple4<String, String, String, Long>>? = null

    override fun open(parameters: Configuration) {
        appEventState = runtimeContext.getState(ValueStateDescriptor("appEventState",
            Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)))

        thirdPartEventState = runtimeContext.getState(ValueStateDescriptor("thirdPartEventState",
            Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)))
        super.open(parameters)
    }

    override fun processElement1(value: Tuple3<String, String, Long>, ctx: Context, out: Collector<String>) {
        // 来的是appEvent，看是否另一条stream有event来过
        if (thirdPartEventState!!.value() != null) {
            out.collect("对账成功:${thirdPartEventState!!.value()}")
            thirdPartEventState!!.clear()
        } else {
            // 更新状态
            appEventState!!.update(value)
            //注册定时器（5s后），等待另一条stream的event
            ctx.timerService().registerEventTimeTimer(value.f2 + 5000L)
        }
    }

    override fun processElement2(value: Tuple4<String, String, String, Long>, ctx: Context, out: Collector<String>) {
        // 来的是thirdPartEvent，看是否另一条stream有event来过
        if (appEventState!!.value() != null) {
            out.collect("对账成功:${appEventState!!.value()}")
            appEventState!!.clear()
        } else {
            // 更新状态
            thirdPartEventState!!.update(value)
            //注册定时器，等待另一条stream的event
            ctx.timerService().registerEventTimeTimer(value.f3)
        }
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<String>) {
        super.onTimer(timestamp, ctx, out)
        // 定时器触发，判断状态，如果某个状态不为空，则对账失败
        if (appEventState!!.value() != null) {
            out.collect("对账失败:${appEventState!!.value()},第三方平台对账失败")
        }
        if (thirdPartEventState!!.value() != null) {
            out.collect("对账失败:${appEventState!!.value()},app信息未收到")
        }
    }
}
