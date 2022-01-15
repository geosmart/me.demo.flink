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

    companion object {
        private const val SMALL_AMOUNT = 1.00
        private const val LARGE_AMOUNT = 500.00
        private const val ONE_MINUTE = (60 * 1000).toLong()
    }

    /***
     * has three states, unset (null), true, and false,
     * because all ValueState’s are nullable.
     */
    @Transient
    var flagState: ValueState<Boolean>? = null

    /***
     * considered fraud if they occurred within 1 minute of each other.
     */
    @Transient
    var timerState: ValueState<Long>? = null

    /***
     * todo when call open()
     */
    override fun open(parameters: Configuration?) {
        logger.info("open function")
        val flagDescriptor = ValueStateDescriptor("flag-state", Types.BOOLEAN)
        flagState = runtimeContext.getState(flagDescriptor)

        val timerDescriptor = ValueStateDescriptor("timer-state", Types.LONG)
        timerState = runtimeContext.getState(timerDescriptor)
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
            cleanup(ctx)
        }
        if (tx.amount < SMALL_AMOUNT) {
            //set flag state
            logger.info("detect small")
            flagState!!.update(true)

            // set a timer for 1 minute in the future every time the flag is set
            // and store the timestamp in timerState.
            val timer = ctx.timerService().currentProcessingTime() + ONE_MINUTE
            ctx.timerService().registerEventTimeTimer(timer)
            timerState!!.update(timer)
        }
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<Alert>) {
        // remove flag after 1 minute
        timerState!!.clear()

        flagState!!.clear()
    }

    private fun cleanup(ctx: Context) {
        //delete timer
        val timer = timerState!!.value()
        ctx.timerService().deleteEventTimeTimer(timer)

        //clean up all state
        timerState!!.clear()
        flagState!!.clear()
    }

}