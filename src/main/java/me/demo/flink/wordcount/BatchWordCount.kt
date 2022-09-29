package me.demo.flink.wordcount

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.util.Collector

class BatchWordCount {
    fun main() {
        //1. 定义环境，默认local
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment()
        //2. 读取数据源
        val wordLines: DataSource<String> = env.readTextFile("data/input/words.txt", Charsets.UTF_8.name())
        //3. 将每行单词进行分词，转换成tuple2
        val wordAndOneTuple = wordLines.flatMap { line: String, out: Collector<Tuple2<Char, Long>> ->
            for (word in line.toCharArray()) {
                out.collect(Tuple2(word, 1L))
            }
        }.returns(Types.TUPLE(Types.CHAR, Types.LONG))
        //4. 按word分组统计sum
        val sumAgg = wordAndOneTuple.groupBy(0).sum(1)
        //5. 结果输出
        sumAgg.writeAsText("file:///mnt/github/me.demo.flink/data/output/word_count.txt").parallelism = 1
    }

}
