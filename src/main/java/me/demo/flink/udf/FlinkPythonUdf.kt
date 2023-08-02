package me.demo.flink.udf

import me.demo.flink.source.ClickSource
import me.demo.flink.source.Event
import org.apache.flink.python.PythonOptions.PYTHON_CLIENT_EXECUTABLE
import org.apache.flink.python.PythonOptions.PYTHON_EXECUTABLE
import org.apache.flink.python.PythonOptions.PYTHON_FILES
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.table.api.Expressions.`$`
import org.apache.flink.table.api.Expressions.call
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.util.Collector

class FlinkPythonUdf {

}

fun main() {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
    val tableEnv = StreamTableEnvironment.create(env)
    //环境准备
    preparePythonEnv(tableEnv)
    preparePythonUdf(tableEnv)

    val stream: SingleOutputStreamOperator<Event> = env.addSource(ClickSource())
        .keyBy { it.user }
        .process(MyProcessFunc())

    val sourceTb: Table = tableEnv.fromDataStream(stream, `$`("user"), `$`("url"), `$`("timestamp"))
    //udf调用
    val sinkTb = sourceTb.select(
        call("metric_udf", `$`("user"), `$`("url"), `$`("timestamp"))
            .`as`("metric")
    )
    sinkTb.execute().print()
}


private fun preparePythonEnv(tableEnv: TableEnvironment) {
    // python环境准备：需安装pip install apache-flink==1.12.2
    val venvPath = "/opt/tool/anaconda3/envs/dboard.venv"
    tableEnv.config.configuration.setString(PYTHON_CLIENT_EXECUTABLE, "$venvPath/bin/python")
    tableEnv.config.configuration.setString(PYTHON_EXECUTABLE, "$venvPath/bin/python")
}

private fun preparePythonUdf(tableEnv: TableEnvironment) {
    // udf准备
    tableEnv.config.configuration.setString(PYTHON_FILES, "/mnt/github/me.demo.flink/src/main/resources/udf/udf.py")
    tableEnv.executeSql("CREATE TEMPORARY SYSTEM FUNCTION metric_udf AS 'udf.metric_udf' LANGUAGE PYTHON")
}


class MyProcessFunc : KeyedProcessFunction<String, Event, Event>() {
    override fun processElement(value: Event, ctx: Context?, out: Collector<Event>) {
        out.collect(value)
    }
}
