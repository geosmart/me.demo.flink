package me.demo.flink.fraud

import me.demo.flink.fraud.FraudDetector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.sink.AlertSink
import org.apache.flink.walkthrough.common.source.TransactionSource

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    //Sources ingest data from external systems into Flink Jobs;
    // such as Apache Kafka, Rabbit MQ, or Apache Pulsar
    val transactions: DataStream<Transaction> = env
        .addSource(TransactionSource())
        .name("transactions")

    //process
    val alerts: DataStream<Alert> = transactions
        .keyBy { it.accountId }
        .process(FraudDetector())
        .name("fraud-detector")

    //A sink writes a DataStream to an external system; 
    // such as Apache Kafka, Cassandra, and AWS Kinesis.
    alerts
        .addSink(AlertSink())
        .name("send-alerts")

    env.execute("fraud-detection")
}
