import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.base.Splitter;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.Collector;


import java.util.Properties;
import  java.lang.String;
import java.util.concurrent.TimeUnit;

public class ReadFromCsv {

    public static void main(String[] args) throws Exception {
        // create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // final TableEnvironment  envtb = TableEnvironment.getTableEnvironment(env);
       // DataStream<String> text = env.readTextFile("hdfs://gpu1:8020//user/zhangxiao/hq.csv");
        DataStream<String> text = env.readTextFile("/user/zhangxiao/hq.csv");

        //DataStream<String> dataStream =  env.readTextFile("file:///home/zhangxiao/mvn/zhangxiao/hq.csv");
        //DataStream<Tuple2<String,Integer>> out1= dataStream.flatMap(new Split());
        text.print();
       // text1.print();
//        DataStream<Integer> parsed = text.map(new MapFunction<String, Integer>() {
//            @Override
//            public Integer map(String value) {
//                return Integer.parseInt(value);
//            }
//        });

        //out.writeAsCsv("file:///home/zhangxiao/mvn/zhangxiao/hq1.csv",FileSystem.WriteMode.OVERWRITE );
       // text1.writeAsCsv("file:///home/zhangxiao/mvn/zhangxiao/hqcsv",FileSystem.WriteMode.NO_OVERWRITE,",","\n");
        env.execute();
    }

    public static class Split implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String,Integer>> out) throws Exception {
            String[] tokens = sentence.split(",");
            for (String token: tokens) {
               // String tradingday="20180104";
                out.collect(new Tuple2<String, Integer>(token,1));
            }
        }
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String,Integer>> out) throws Exception {
            for (String tradingday: sentence.split("/n")) {
                out.collect(new Tuple2<String, Integer>(tradingday,1));
            }
        }
    }
}