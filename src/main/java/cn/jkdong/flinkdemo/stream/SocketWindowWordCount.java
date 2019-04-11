package cn.jkdong.flinkdemo.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
public class SocketWindowWordCount {

	public static void main(String[] args) throws Exception {

		final String hostname;
		final int port=9000;
		try {
			// 入参检查
			final ParameterTool params = ParameterTool.fromArgs(args);
			// hostname = params.has("hostname") ? params.get("hostname") : "47.100.240.63";
			hostname = params.has("hostname") ? params.get("hostname") : "127.0.0.1";
		} catch (Exception e) {
			System.err.println("请输入地址和端口...");
			return;
		}

		// 运行时
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 传入数据
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");

		// 数据解析
		DataStream<WordWithCount> windowCounts = text
				.flatMap(new FlatMapFunction<String, WordWithCount>() {
					@Override
					public void flatMap(String value, Collector<WordWithCount> out) {
						for (String word : value.split("\\s")) {
							out.collect(new WordWithCount(word, 1L));
						}
					}
				})
				.keyBy("word")
				.timeWindow(Time.seconds(10))
				.reduce(new ReduceFunction<WordWithCount>() {
					@Override
					public WordWithCount reduce(WordWithCount a, WordWithCount b) {
						return new WordWithCount(a.word, a.count + b.count);
					}
				});

		// 结果打印
		windowCounts.print().setParallelism(1);

		env.execute("wc-stream-socket");
	}


	// po
	public static class WordWithCount {

		public String word;
		public long count;

		public WordWithCount() {}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}
}
