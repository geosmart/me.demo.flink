package me.demo.flink.source

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.util.Random

class ParallelCustomSource : ParallelSourceFunction<Int> {

    var running = true
    private val random = Random()

    override fun run(ctx: SourceFunction.SourceContext<Int>) {
        while (running) {
            //随机模拟数据
            ctx.collect(random.nextInt())
        }
    }

    override fun cancel() {
        running = false
    }


}
