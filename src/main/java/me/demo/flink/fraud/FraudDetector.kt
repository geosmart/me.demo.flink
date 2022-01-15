package me.demo.flink.fraud

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class FraudDetector : KeyedProcessFunction<Long, Transaction, Alert>() {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    override fun processElement(value: Transaction, ctx: Context, out: Collector<Alert>) {
        val alert = Alert()
        alert.id = value.accountId
        out.collect(alert)
    }

}