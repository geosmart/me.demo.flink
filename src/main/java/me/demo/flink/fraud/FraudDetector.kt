package me.demo.flink.fraud

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class FraudDetector : KeyedProcessFunction<Long, Transaction, Alert>() {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val SMALL_AMOUNT = 1.00
    private val LARGE_AMOUNT = 500.00
    private val ONE_MINUTE = (60 * 1000).toLong()

    /***
     * has three states, unset (null), true, and false,
     * because all ValueState’s are nullable.
     */
    @Transient
    var flagState: ValueState<Boolean>? = null

    override fun open(parameters: Configuration?) {
        logger.info("open function")
        val stateDescriptor = ValueStateDescriptor("flag", Types.BOOLEAN)
        flagState = runtimeContext.getState(stateDescriptor)
    }

    /***
     * the fraud detector should output an alert for any account that
     * makes a small transaction immediately followed by a large one.
     */
    override fun processElement(tx: Transaction, ctx: Context, out: Collector<Alert>) {
        val lastTxWasSmall = flagState!!.value()
        //check if the flag is set
        if (lastTxWasSmall != null) {
            if (tx.amount > LARGE_AMOUNT) {
                logger.info("detect large")
                val alert = Alert()
                alert.id = tx.accountId
                out.collect(alert)
            }
            flagState!!.clear()
        }
        if (tx.amount < SMALL_AMOUNT) {
            logger.info("detect small")
            flagState!!.update(true)
        }
    }

}