package me.demo.flink.wordcount

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import java.util.Properties

fun main(args: Array<String>) {
    //source:kafka
    val properties = Properties()
    properties.setProperty("bootstrap.servers", "10.199.150.199:9092")
    properties.setProperty("group.id", "flink_word_count")
    val inputTopic = "flink_debug"
    val consumer = FlinkKafkaConsumer(inputTopic, SimpleStringSchema(), properties)
    consumer.setStartFromEarliest()

    //compute environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val stream: DataStream<String> = env.addSource(consumer)

    //transformer
    val wordCount: DataStream<Tuple2<String, Int>?> = stream.flatMap { line: String, collector: Collector<Tuple2<String, Int>?> ->
        val tokens = line.split(",").toTypedArray()
        for (token in tokens) {
            if (token.isNotEmpty()) {
                collector.collect(Tuple2(token, 1))
            }
        }
    }.returns(Types.TUPLE(Types.STRING, Types.INT))
        .keyBy(0)
        .timeWindow(Time.seconds(20))
        .sum(1)
    //sink:console
    wordCount.print()

    //action
    env.execute("kafka streaming world count")
}