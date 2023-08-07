package me.demo.flink.split

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.ServerSocket

/***
 * 参考：https://www.bilibili.com/video/BV133411s7Sa?p=80
 */
fun executeCommand(command: String?): String {
    // 在这里执行命令，并返回结果
    return "Command executed: $command"
}

fun main() {
    val port = 12345 // 定义端口号

    try {
        val serverSocket = ServerSocket(port) // 创建ServerSocket对象，绑定端口号

        println("Listening on port $port...")

        while (true) {
            val clientSocket = serverSocket.accept() // 等待客户端连接

            println("Client connected: ${clientSocket.inetAddress.hostAddress}")

            // 创建输入输出流
            val reader = BufferedReader(InputStreamReader(clientSocket.getInputStream()))
            val writer = PrintWriter(clientSocket.getOutputStream(), true)

            // 处理客户端请求
            var line: String?
            while (reader.readLine().also { line = it } != null) {
                println("Received: $line")

                // 处理命令
                val command = line?.trim()
                if (command.equals("quit", ignoreCase = true)) {
                    break
                } else {
                    // 执行命令并返回结果
                    val result = executeCommand(command)
                    writer.println(result)
                }
            }

            // 关闭连接
            reader.close()
            writer.close()
            clientSocket.close()

            println("Client disconnected: ${clientSocket.inetAddress.hostAddress}")
        }

        serverSocket.close()
    } catch (e: Exception) {
        e.printStackTrace()
    }
}
