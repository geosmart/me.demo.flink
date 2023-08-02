package me.demo.flink.report

import me.demo.flink.report.SpendReport.report
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.TableEnvironment

object SpendReport {

    @JvmStatic
    fun report(transactions: Table?): Table {
        throw UnimplementedException()
    }
}

fun main() {
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
        .inStreamingMode()
        .build()
    val tEnv: TableEnvironment = TableEnvironment.create(settings)

    tEnv.executeSql("""CREATE TABLE transactions (
    account_id  BIGINT,
    amount      BIGINT,
    transaction_time TIMESTAMP(3),
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic'     = 'transactions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'    = 'csv'
)""")

    tEnv.executeSql("""CREATE TABLE spend_report (
    account_id BIGINT,
    log_ts     TIMESTAMP(3),
    amount     BIGINT
,    PRIMARY KEY (account_id, log_ts) NOT ENFORCED) WITH (
   'connector'  = 'jdbc',
   'url'        = 'jdbc:mysql://mysql:3306/sql-demo',
   'table-name' = 'spend_report',
   'driver'     = 'com.mysql.jdbc.Driver',
   'username'   = 'sql-demo',
   'password'   = 'demo-sql'
)""")

    val transactions: Table = tEnv.from("transactions")
    report(transactions).executeInsert("spend_report")
}
