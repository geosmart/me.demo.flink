package me.demo.flink.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//me.demo.flink.wordcount.JavaWordCount
public class JavaWordCount {

    public static void main(String[] args) {
        //1. 定义环境，默认local
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2. 读取数据源
        DataSource<String> wordLines = env.readTextFile("data/input/words.txt", "utf-8");
        //3. 将每行单词进行分词，转换成tuple2
        wordLines.flatMap(
            (String line, Collector<Tuple2<Character, Long>> out) -> {
                for (Character word : line.toCharArray()) {
                    out.collect(new Tuple2(word, 1L));
                }
            })
            .returns(Types.TUPLE(Types.CHAR, Types.LONG))
            .groupBy(0)
            .sum(1)
            .writeAsText("file:///mnt/github/me.demo.flink/data/output/word_count.txt");
    }
}
