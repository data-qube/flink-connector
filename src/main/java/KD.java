//import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.*;
import java.util.*;

/**
 * @Author ZhangXiao
 * @Date 2018/1/15  09:43
 * cmd ./quote_data_producer -t hq -b 192.168.130.141:6667
 * flink  http://flink.apache.org/visualizer/
 **/

public class KD {
    public static void main(String[] args) throws Exception {

        final  String sourceTopic ="hqtest";

        final  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();
//        env.setStateBackend(new FsStateBackend("hdfs://gpu1:8020/flink/checkpoints"));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01.io:9000/flink/checkpoints"));
        env.enableCheckpointing(5000); //
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.130.141:6667");
        env.setParallelism(1);
//        properties.setProperty("zookeeper.connect", "192.168.130.141:2181,192.168.130.142:2181,192.168.130.143:2181");
        properties.setProperty("group.id", "kafka-demo");
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(sourceTopic, new SimpleStringSchema(), properties);
        myConsumer.assignTimestampsAndWatermarks(new MyWatermarkEmitter());
        myConsumer.setStartFromEarliest()  ;
        DataStream<String> stream = env.addSource(myConsumer);
        DataStream<Tuple8<String,String,String, Double, Double, Double, Double,Long>>
                streamTuple= stream.flatMap(new MySplitter())
                .filter(new FilterFunction<Tuple8<String,String, String,Double, Double, Double, Double,Long>>() {
                    @Override
                    public boolean filter(Tuple8<String,String, String,Double, Double, Double, Double,Long> value) throws Exception {
                        return ((value.f3 > 0)&& (value.f4> 0)&& (value.f5> 0)&& (value.f6> 0));
                    }
                }).setParallelism(1);
        streamTuple.print();

        try {
            env.execute("KD");
            //System.out.println(env.getExecutionPlan());
        } catch (Exception ex) {

            ex.printStackTrace();
        }
    };
}
