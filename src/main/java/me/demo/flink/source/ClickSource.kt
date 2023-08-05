package me.demo.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.util.Calendar
import java.util.Random
import java.util.concurrent.TimeUnit
import java.util.stream.IntStream

class ClickSource : SourceFunction<Event> {

    var running = true

    override fun run(ctx: SourceFunction.SourceContext<Event>) {
        val users = listOf("mary", "lily", "alice", "bob", "cary")
        val urls = listOf("./home", "./cart", "./fav", "./prod?id=100", "./prod?id=10")
        val random = Random()

        while (running) {
            //随机模拟数据
            val mockEvent = Event(
                users[random.nextInt(users.size)],
                urls[random.nextInt(urls.size)],
                Calendar.getInstance().timeInMillis - 100000
            )

            IntStream.range(0, 4).parallel().forEach {
                ctx.collect(mockEvent)
            }
            println(mockEvent)
        }
    }

    override fun cancel() {
        running = false
    }


}
