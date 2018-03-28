import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.Properties;


/**
 * @Author ZhangXiao
 * @Date 2018/1/15  09:43
 * 命令操作 ./quote_data_producer -t hq -b 192.168.130.141:6667
 * flink 执行计划可视化路径 http://flink.apache.org/visualizer/
 * 此类作为功能展示，功能糅合在了一起，包括，通过kafka读取数据，读完数据之后做基本的处理，处理完的数据落hbase或者kafka
 **/

public class FlinkHbase {
    //此处初始化参数，包括可以建立Hashmap存储报警相关参数
    private static TableName tableName = TableName.valueOf("zx");
    private static final String columnFamily = "cf";

    public static void main(String[] args) throws Exception {
         ConnectHbase connect  = null;

        final String ZOOKEEPER_HOST = "192.168.130.141:2181,192.168.130.142:2181,192.168.130.143:2181";
        final String KAFKA_HOST = "192.168.130.141:6667";

        final  String sourceTopic ="hqtest";
        final  String sinkTopic = "kd-hbase";
        final  Time windowTime = Time.seconds(60);
        final  Time slideTime  = Time.seconds(60);
        final  Time Latency  = Time.seconds(3);

        final String  starttime = "093000";
        final String  endtime = "150000";
        final  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);
        env.setBufferTimeout(10);//网络传输控制延时，设置-1是表明， 必须非buffer满时才flush
        //性能优化，开启对象复用
        env.getConfig().enableObjectReuse();
        //env.setStateBackend(new FsStateBackend("hdfs://gpu1:8020/flink/checkpoints"));
        env.enableCheckpointing(5000); // 每5000毫秒检查一次
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.130.141:6667");
        properties.setProperty("zookeeper.connect", "192.168.130.141:2181,192.168.130.142:2181,192.168.130.143:2181");
        properties.setProperty("group.id", "kafka-hbase");
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(sourceTopic, new SimpleStringSchema(), properties);
        myConsumer.assignTimestampsAndWatermarks(new MyWatermarkEmitter());
        //FlinkKafkaProducer010<String> produce = new FlinkKafkaProducer010<String>(sinkTopic, new SimpleStringSchema(), properties);
        myConsumer.setStartFromEarliest() ;
        //myConsumer.setStartFromLatest() ;
        DataStream<String> stream = env.addSource(myConsumer).setParallelism(1);
        DataStream<Tuple8<String,String,String, Double, Double, Double, Double,Long>>
                streamTuple= stream.flatMap(new MySplitter())
        .filter(new FilterFunction<Tuple8<String,String, String,Double, Double, Double, Double,Long>>() {
            @Override
            public boolean filter(Tuple8<String,String, String,Double, Double, Double, Double,Long> value) throws Exception {
                return ((value.f3 > 0)&& (value.f4> 0)&& (value.f5> 0)&& (value.f6> 0));
            }
        });

        DataStream<Tuple6<String,String, String,String, String, String>> result = streamTuple
                .keyBy(0).timeWindow(windowTime,slideTime).allowedLateness(Latency)
                .apply(new KDTimeTuple()).startNewChain();
        result.map(new MapFunction<Tuple6<String,String,String,String,String,String>, String>() {
            ConnectHbase connect  = null;
            @Override
            public String map(Tuple6<String, String, String, String, String, String> value) throws Exception {
                String result = value.f0+","+value.f1+","+value.f2+","+value.f3+
                        ","+value.f4+","+value.f5;
                if( null == connect  ){
                    connect =new ConnectHbase() ;
                }
                connect.writeIntoHBase(result);
                System.out.println(result);
                return result;
            }
        });
        try {
               env.execute("FlinkHbase");
               //System.out.println(env.getExecutionPlan());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        };
}
