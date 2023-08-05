package table

import me.demo.flink.table.ChangeLogRowSource
import me.demo.flink.table.XiaoxinQuestion
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.annotation.FunctionHint
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind
import org.junit.Test
import org.slf4j.LoggerFactory

// 单个table的changelog，sink到1张表
class TableSinkTest {

    @Test
    fun toChangelogStream() {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 1
        val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
        //tableEnv.config.configuration.setString("table.exec.sink.not-null-enforcer", "drop")
        tEnv.config.configuration.setString("execution.checkpointing.interval", "30 s")

        val stream1: DataStream<Row> = env.addSource(ChangeLogRowSource()).returns(XiaoxinQuestion.typeInfo)
        // changelog table
        val tb2: Table = tEnv.fromChangelogStream(stream1, XiaoxinQuestion.schema, ChangelogMode.upsert())
        tb2.printSchema()
        tEnv.createTemporaryView("StreamXiaoxinQuestion", tb2)
        defineSinkTable(tEnv)

        tEnv.createTemporarySystemFunction("ProcessFunc", ProcessFunc::class.java)
        //fixme  changelog 是否会传入 udf，不会！！
        val sql = """
            SELECT  T.* FROM StreamXiaoxinQuestion as a
            JOIN LATERAL TABLE(
                ProcessFunc(
                    a.id,
                    a.robot_id,
                    a.standard_question_id,
                    a.hf_id,
                    a.user_question,
                    a.question_type,
                    a.menu_id,
                    a.menu_retrieve,
                    a.is_standard_question
                )
            ) as T(id,
                    robot_id,
                    standard_question_id,
                    hf_id,
                    user_question,
                    question_type,
                    menu_id,
                    menu_retrieve,
                    is_standard_question)
            ON TRUE
        """.trimIndent()
        val resTb = tEnv.sqlQuery(sql)
        val res = resTb.executeInsert("JdbcXiaoxinQuestion")
        res.print()
    }

    private fun defineSinkTable(tEnv: StreamTableEnvironment) {
        val writeSql = """
                CREATE TABLE JdbcXiaoxinQuestion (
                id STRING,
                robot_id STRING,
                standard_question_id STRING,
                hf_id STRING,
                user_question STRING,
                question_type STRING,
                menu_id STRING,
                menu_retrieve STRING,
                is_standard_question BOOLEAN,
                PRIMARY KEY (id) NOT ENFORCED
                )
                WITH (
                   'connector' = 'jdbc',
                   'driver' = 'com.mysql.cj.jdbc.Driver',
                   'url' = 'jdbc:mysql://127.0.0.1:3306/flink_demo',
                   'username' = 'root',
                   'password' = '123456',
                   'sink.buffer-flush.max-rows' = '500',
                   'sink.buffer-flush.interval' = '10000',
                   'sink.max-retries' = '3',
                   'table-name' = 'xiaoxin_question'
                )
            """.trimIndent()
        tEnv.executeSql(writeSql)
    }
}

@FunctionHint(
        input = [
            DataTypeHint("STRING"),
            DataTypeHint("STRING"),
            DataTypeHint("STRING"),
            DataTypeHint("STRING"),
            DataTypeHint("STRING"),
            DataTypeHint("STRING"),
            DataTypeHint("STRING"),
            DataTypeHint("STRING"),
            DataTypeHint("BOOLEAN")
        ],
        output = DataTypeHint("""ROW<
                id STRING,
                robot_id STRING,
                standard_question_id STRING,
                hf_id STRING,
                user_question STRING,
                question_type STRING,
                menu_id STRING,
                menu_retrieve STRING,
                is_standard_question BOOLEAN
            >""")
)
class ProcessFunc : TableFunction<Row>() {
    private val logger = LoggerFactory.getLogger(this::class.java)

    // todo 如何处理删除事件

    /**
     * udf调用实际执行函数
     */
    @Throws(Exception::class)
    fun eval(id: String,
             robot_id: String,
             standard_question_id: String?,
             hf_id: String?,
             user_question: String,
             question_type: String,
             menu_id: String?,
             menu_retrieve: String?,
             is_standard_question: Boolean) {
        val row = if (id.equals("1")) {
            Row.ofKind(RowKind.UPDATE_AFTER, id, "robot-update1", System.currentTimeMillis().toString(), hf_id, user_question, question_type, menu_id, menu_retrieve, is_standard_question)
        } else {
            Row.ofKind(RowKind.UPDATE_AFTER, id, "robot-update2", System.currentTimeMillis().toString(), hf_id, user_question, question_type, menu_id, menu_retrieve, is_standard_question)
        }
        collect(row)
        logger.info("udf处理：${row.getField(0)}")
    }
}
