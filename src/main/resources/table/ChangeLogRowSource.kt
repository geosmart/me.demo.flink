package me.demo.flink.table

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind
import java.util.Random

class ChangeLogRowSource : SourceFunction<Row> {

    var running = true

    override fun run(ctx: SourceFunction.SourceContext<Row>) {
        val random = Random()

        while (running) {
            //随机模拟数据
            val mockEvent = XiaoxinQuestion(NoMatchQuestion().apply {
                id = "hfId-${random.nextInt(10)}"
                robot_id = "robot-${random.nextInt(5)}"
                question = "question-xxx"
            })
//            println("event:" + mockEvent.toString())

            ctx.collect(XiaoxinQuestion(NoMatchQuestion().apply {
                id = "1"
                robot_id = "robot-1"
                question = "question-xxx"
            }).toRow(RowKind.INSERT))

            ctx.collect(XiaoxinQuestion(NoMatchQuestion().apply {
                id = "1"
                robot_id = "robot-1"
                question = "question-xxx"
            }).toRow(RowKind.UPDATE_AFTER))

            ctx.collect(XiaoxinQuestion(NoMatchQuestion().apply {
                id = "2"
                robot_id = "robot-2"
                question = "question-xxx"
            }).toRow(RowKind.INSERT))
//            when {
//                Calendar.getInstance().timeInMillis % 2 == 0L -> {
//                    ctx.collect(mockEvent.toRow(RowKind.DELETE))
//                }
//                else -> {
//                    ctx.collect(mockEvent.toRow(RowKind.INSERT))
//                }
//            }
            Thread.sleep(1000L)
        }
    }

    override fun cancel() {
        running = false
    }

}
