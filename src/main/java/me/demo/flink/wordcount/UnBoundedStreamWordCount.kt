package me.demo.flink.wordcount

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

class UnBoundedStreamWordCount {
    fun main() {
        //1. 定义stream环境，默认local
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
        //2. 读取socket文本流
        val lineDataStream: DataStreamSource<String> = env.socketTextStream("localhost", 7777)
        //3. 将每行单词进行分词，转换成tuple2
        val wordAndOneTuple: SingleOutputStreamOperator<Tuple2<Char, Long>> = lineDataStream
            .flatMap { line: String, out: Collector<Tuple2<Char, Long>> ->
                for (word in line.toCharArray()) {
                    out.collect(Tuple2(word, 1L))
                }
            }.returns(Types.TUPLE(Types.CHAR, Types.LONG))
        //4. 按word分组统计sum,默认parallelism为cpu线程数
        val sumAgg = wordAndOneTuple.keyBy { data -> data.f0 }.sum(1)

        sumAgg.print()
        //执行流程序
        env.execute()
    }
}
