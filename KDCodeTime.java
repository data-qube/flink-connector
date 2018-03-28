import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * @Author ZhangXiao
 * @Date 2018/1/15  09:43
 **/
public class KDCodeTime {
    public static void main(String[] args) throws Exception {

        final  Time windowTime = Time.seconds(60);
        final  Time slideTime = Time.seconds(60);
        final  Time Latency  = Time.seconds(3);
        //final OutputTag<String> lateOutputTag = new OutputTag<String>("late-data"){};

        final  String sourceTopic ="hqtest";
        final  String sinkTopic = "kd-zx";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.130.141:6667");
        properties.setProperty("zookeeper.connect", "192.168.130.141:2181,192.168.130.142:2181,192.168.130.143:2181");
        properties.setProperty("group.id", "kafka-kdcodetime");
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(sourceTopic, new SimpleStringSchema(), properties);
        FlinkKafkaProducer010<String> produce = new FlinkKafkaProducer010<String>(sinkTopic, new SimpleStringSchema(), properties);

        final  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new FsStateBackend("hdfs://gpu1:8020/flink/checkpoints"));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10000); // 每5000毫秒检查一次
        myConsumer.assignTimestampsAndWatermarks(new MyWatermarkEmitter());
        myConsumer.setStartFromLatest();
        DataStream<String> stream = env.addSource(myConsumer);

        DataStream<Tuple8<String,String,String, Double, Double, Double, Double,Long>>
                streamTuple= stream.flatMap(new MySplitter())
                .filter(new FilterFunction<Tuple8<String,String, String,Double, Double, Double, Double,Long>>() {
                    @Override
                    public boolean filter(Tuple8<String,String, String,Double, Double, Double, Double,Long> value) throws Exception {
                        return ((value.f3 > 0)&& (value.f4> 0)&& (value.f5> 0)&& (value.f6> 0));
                    }
                }).setParallelism(10);
        DataStream<String> result = streamTuple.keyBy(0).timeWindow(windowTime,slideTime).allowedLateness(Latency)
                .apply(new KDTimeWindowCodeTime()).startNewChain().setParallelism(10);
        result.addSink(produce);

        try {
            env.execute("KDCodeTime");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    };
}
