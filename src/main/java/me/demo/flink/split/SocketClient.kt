package me.demo.flink.split

import cn.hutool.core.io.FileUtil

import java.net.Socket

fun main() {
    val mockEvents = FileUtil.readUtf8Lines("data/test_source_event.csv")
    // 向一个socket端口发送上面的events
    Thread {
        val socket = Socket("localhost", 12345)
        val writer = socket.getOutputStream()
        mockEvents.forEach {
            println("send: $it")
            writer.write("$it\n".toByteArray())
            writer.flush()
            Thread.sleep(1000)
        }
        writer.close()
    }.start()
}
