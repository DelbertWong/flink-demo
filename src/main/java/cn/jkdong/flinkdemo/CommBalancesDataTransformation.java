package cn.jkdong.flinkdemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * @author: dongdb
 * @date: 2019/3/12
 * @Description: 商户余额明细处理
 */
public class CommBalancesDataTransformation {

    public static void main(String[] args) throws Exception {
        long t1 = new Date().getTime();
        // 缺省路径
        String inputDefault="D:\\tmp2";
        String outputDefault="D:\\tmp2\\commbalances.csv";
        // String inputDefault="/mnt/xvdb/upload/commbalances";
        // String outputDefault="/mnt/xvdb/upload/commbalances/commbalances.csv";

        final ParameterTool params=ParameterTool.fromArgs(args);
        // flink运行时
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataSet<String> text;
        // 读取输入
        if (params.has("input")){
            text = env.readTextFile(params.get("input"));
        }else {
            System.out.println("没有输入路径，使用缺省输入>>>>"+inputDefault);
            text = env.readTextFile(inputDefault);
        }
        // 按第一个元组元素分组取第二个元组元组的和值  ：groupby userid max(time)
        DataSet<Tuple2<String, Long>> sum = text.flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String,Long>> out) throws Exception {
                //每行数据组成的数组
                String[] lines = value.split("\n");
                // 遍历各行（一行就是一个对象）数据
                for (String line : lines) {
                    String[] words=line.split(",");
                    if (words.length>1){
                        // 截取merchantId tradePrice
                        String merchantId="";
                        Long tradePrice=0L;

                        if (words[0]!=null && words[0].length()>11){
                            merchantId = words[0].substring(11);
                        }
                        if (words[1]!=null && words[1].length()>11){
                            tradePrice = Long.valueOf(words[1].substring(11));
                        }
                        //一行数据
                        // 拼装tuple2 : merchantId tradePrice
                        if (line.length()>0){
                            out.collect(new Tuple2<>(merchantId,tradePrice));
                        }
                    }
                }
            }
        }).groupBy(0).sum(1);
        // 射出结果
        if (params.has("output")){
            sum.writeAsCsv(params.get("output")).setParallelism(1);
        }else {
            System.out.println("没有射出结果，使用缺省输出>>>>"+outputDefault);
            sum.writeAsCsv(outputDefault).setParallelism(1);
        }
        // 任务执行
        env.execute("batch success");
        long t2 = new Date().getTime();
        System.out.println("任务总用时>>>>>>>"+(t2-t1)/100+"秒");

        // 运行命令  /mnt/xvdb/flink/flink/bin/flink run -c com.mamcharge.commbalances.CommBalancesDataTransformation /mnt/xvdb/flink_jars/comm_balances_trans.jar --input
    }

}
