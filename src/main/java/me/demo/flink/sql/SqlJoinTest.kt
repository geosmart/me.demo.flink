package me.demo.flink.sql

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import java.sql.Timestamp
import java.time.Duration
import java.util.Calendar
import java.util.UUID

fun main() {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.parallelism = 1

    val orderStream: SingleOutputStreamOperator<Order> = env
        .addSource(OrderSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness<Order>(Duration.ofSeconds(0))
                .withTimestampAssigner { element, _ -> element.timestamp }
        )

    val callBackStream: SingleOutputStreamOperator<OrderCallback> = env
        .addSource(OrderCallbackSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness<OrderCallback>(Duration.ofSeconds(0))
                .withTimestampAssigner { element, _ -> element.timestamp }
        )

    val tableEnv = StreamTableEnvironment.create(env)

    // 订单表
    var orderTb: Table = tableEnv.fromDataStream(
        orderStream,
        "id as id, predictionId as predictionId,timestamp as timestamp"
    )
    orderTb.printSchema()
    tableEnv.createTemporaryView("t_order", orderTb)
    // 时间戳类型转换
    orderTb = tableEnv.sqlQuery(
        """
        select id,predictionId,TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000, 'yyyy-MM-dd HH:mm:ss')) as ts
        from t_order
    """.trimIndent()
    )
    orderTb.printSchema()
    tableEnv.createTemporaryView("v_order", orderTb)

    //回流表
    var callbackTb: Table = tableEnv.fromDataStream(
        callBackStream,
        """
            id as id,
            predictionId as predictionId,
            result as result,
            timestamp as timestamp
        """.trimIndent()
    )
    callbackTb.printSchema()
    // 时间戳类型转换
    tableEnv.createTemporaryView("t_callback", callbackTb)
    callbackTb = tableEnv.sqlQuery(
        """
        select
        `id`,
        `predictionId`,
        `result`,
        TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000, 'yyyy-MM-dd HH:mm:ss')) as ts
        from t_callback
    """.trimIndent()
    )
    tableEnv.createTemporaryView("v_callback", callbackTb)
    //时间类型转换
    callbackTb.printSchema()

    // 结果记录数为20条
    // explain:	Join(joinType=[InnerJoin], where=[((predictionId = predictionId0) AND (ts0 >= ts) AND (ts0 <= (ts + 6000:INTERVAL SECOND)))],
    watermarkIntervalCase1(tableEnv)

    // 结果记录数为20条
    // explain: Join(joinType=[InnerJoin], where=[((predictionId = predictionId0) AND (ts >= (ts0 - 6000:INTERVAL SECOND)) AND (ts <= ts0))],
    // watermarkIntervalCase2(tableEnv)
}

private fun watermarkIntervalCase1(tableEnv: StreamTableEnvironment) {
    val table: Table = tableEnv.sqlQuery(
        """
        SELECT
            t1.*,
            t2.ts as callbackTs,
            t2.predictionId as callbackPredictionId,
            t2.id as callbackId,
            t2.`result`  as callbackResult
        FROM
            v_order t1
            LEFT JOIN v_callback t2 ON t1.`predictionId` = t2.`predictionId`
             AND (
                t2.`ts` BETWEEN  t1.`ts` AND  t1.`ts` + INTERVAL '6' SECOND
                )
            where  t2.`result` is not null
    """.trimIndent()
    )
    println(table.explain())
    table.execute().print()
}

private fun watermarkIntervalCase2(tableEnv: StreamTableEnvironment) {
    val table: Table = tableEnv.sqlQuery(
        """
        SELECT
            t1.*,
            t2.ts as callbackTs,
            t2.predictionId as callbackPredictionId,
            t2.id as callbackId,
            t2.`result`  as callbackResult
        FROM
            v_order t1
            LEFT JOIN v_callback t2 ON t1.`predictionId` = t2.`predictionId`
             AND (
                t1.`ts` BETWEEN t2.`ts` - INTERVAL '6' SECOND AND t2.`ts`
                )
            where  t2.`result` is not null
    """.trimIndent()
    )
    println(table.explain())
    table.execute().print()
}

/***
 * 订单回流事件源
 */
class OrderCallbackSource : SourceFunction<OrderCallback> {

    var running = true

    override fun run(ctx: SourceFunction.SourceContext<OrderCallback>) {
        //回流数据5s延时
        Thread.sleep(5000)
        for (i in 1..20) {
            // 订单触发后，5s后产生回调事件
            //随机模拟数据
            val mockEvent = OrderCallback(
                i.toString(),
                "true",
                Calendar.getInstance().timeInMillis
            )
            ctx.collect(mockEvent)
            println(mockEvent)
            Thread.sleep(1000)
        }
    }

    override fun cancel() {
        running = false
    }
}

/***
 * 订单回流事件
 */
open class OrderCallback {
    lateinit var id: String
    lateinit var predictionId: String
    lateinit var result: String

    var timestamp: Long = 0

    constructor()

    constructor(predictionId: String, result: String, timestamp: Long) {
        this.id = UUID.randomUUID().toString()
        this.predictionId = predictionId
        this.result = result
        this.timestamp = timestamp
    }

    override fun toString(): String {
        return "回流事件(predictionId='$predictionId',  timestamp=${Timestamp(timestamp)})"
    }
}

/***
 * 订单事件源
 */
class OrderSource : SourceFunction<Order> {

    var running = true

    override fun run(ctx: SourceFunction.SourceContext<Order>) {
        for (i in 1..20) {
            //随机模拟数据
            val mockEvent = Order(
                i.toString(),
                Calendar.getInstance().timeInMillis
            )
            ctx.collect(mockEvent)
            println(mockEvent)
            // 阻塞等待回流事件触发
            Thread.sleep(1000)
        }
    }

    override fun cancel() {
        running = false
    }
}


/***
 * 订单事件
 */
open class Order {
    lateinit var id: String
    lateinit var predictionId: String


    var timestamp: Long = 0

    constructor()

    constructor(predictionId: String, timestamp: Long) {
        this.id = UUID.randomUUID().toString()
        this.predictionId = predictionId
        this.timestamp = timestamp
    }

    override fun toString(): String {
        return "等待事件(predictionId='$predictionId',  timestamp=${Timestamp(timestamp)})"
    }
}


