package cn.jkdong.flinkdemo.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {

        // 入参检查
        final ParameterTool params = ParameterTool.fromArgs(args);

        // 运行时
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 参数
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text;
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("没有传入数据，使用缺省数据源...WordCountData");
            text = env.fromElements(WordCountData.WORDS);
        }

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new MyFunction())
                        .keyBy(0).sum(1);

        // 弹出结果
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("没有传入弹出路径，缺省控制台打印...");
            counts.print();
        }

        // 程序执行
        env.execute("wc-stream");
    }


    // 自定义function
    public static final class MyFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

}
