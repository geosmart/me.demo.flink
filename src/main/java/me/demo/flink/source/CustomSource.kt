package me.demo.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.util.Calendar
import java.util.Random

 class CustomSource : SourceFunction<Event> {

    var running = true

    override fun run(ctx: SourceFunction.SourceContext<Event>) {
        val users = listOf("mary", "alice", "bob", "cary")
        val urls = listOf("./home", "./cart", "./fav", "./prod?id=100", "./prod?id=10")
        val random = Random()

        while (running) {
            //随机模拟数据
            val mockEvent = Event(
                users[random.nextInt(users.size)],
                urls[random.nextInt(urls.size)],
                Calendar.getInstance().timeInMillis)
            ctx.collect(mockEvent)
            Thread.sleep(1000)
        }
    }

    override fun cancel() {
        running = false
    }


}
