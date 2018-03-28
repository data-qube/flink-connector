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

public class processKD {

    private static TableName tableName = TableName.valueOf("zx");
    private static final String columnFamily = "cf";
    private static ConnectHbase connect ;
    private DataStream<Tuple8<String, String, String, Double, Double, Double, Double, Long>> streamTuple;

    public static void main(String[] args) throws Exception {
       // System.out.println("Start Creating Connection");
       // if( null == connect  ){
        //    connect =new ConnectHbase() ;
       // }
       // System.out.println("Connection Created");
        //final String ZOOKEEPER_HOST = "192.168.130.141:2181,192.168.130.142:2181,192.168.130.143:2181";
       // final String KAFKA_HOST = "192.168.130.141:6667";
        //ParameterTool  parameterTool =ParameterTool.fromArgs(args);
        //flink ui
        //String input = parameterTool.getRequired("input");
        //String output = parameterTool.getRequired("output");

        final  String sourceTopic ="hqtest";
        final  String sinkTopic = "kd-zx";
        final  Time windowTime = Time.seconds(60);
        final  Time slideTime  = Time.seconds(60);
        final  Time Latency  = Time.seconds(3);
        final  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();
        env.setStateBackend(new FsStateBackend("hdfs://gpu1:8020/flink/checkpoints"));
        env.enableCheckpointing(10000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.130.141:6667");
        properties.setProperty("zookeeper.connect", "192.168.130.141:2181,192.168.130.142:2181,192.168.130.143:2181");
        properties.setProperty("group.id", "kafka-demo");
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(sourceTopic, new SimpleStringSchema(), properties);
        //FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(topic, new TypeInformationSerializationSchema(), properties);
        myConsumer.assignTimestampsAndWatermarks(new MyWatermarkEmitter());
        FlinkKafkaProducer010<String> produce = new FlinkKafkaProducer010<String>(sinkTopic, new SimpleStringSchema(), properties);
//        kafka
//        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
//        specificStartOffsets.put(new KafkaTopicPartition("kafka-demo", 0), 23L);
//        specificStartOffsets.put(new KafkaTopicPartition("kafka-demo", 1), 31L);
//        specificStartOffsets.put(new KafkaTopicPartition("kafka-demo", 2), 43L);
//        myConsumer.setStartFromSpecificOffsets(specificStartOffsets);
        myConsumer.setStartFromEarliest()  ;    // start from the earliest record possible
       // myConsumer.setStartFromLatest()     ;   // start from the latest record
      //  myConsumer.setStartFromGroupOffsets();  // the default behaviour
       // myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
        DataStream<String> stream = env.addSource(myConsumer);
        DataStream<Tuple8<String,String,String, Double, Double, Double, Double,Long>>
                streamTuple= stream.flatMap(new MySplitter())
        .filter(new FilterFunction<Tuple8<String,String, String,Double, Double, Double, Double,Long>>() {
            @Override
            public boolean filter(Tuple8<String,String, String,Double, Double, Double, Double,Long> value) throws Exception {
                return ((value.f3 > 0)&& (value.f4> 0)&& (value.f5> 0)&& (value.f6> 0));
            }
        }).setParallelism(2);
        /* process time data */
//        streamTuple.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(300))).allowedLateness(Time.seconds(0)).min("f2")
//                .filter(new FilterFunction<Tuple8<String,String, String,Double, Double, Double, Double,Long>>() {
//            @Override
//            public boolean filter(Tuple8<String,String, String,Double, Double, Double, Double,Long> value) throws Exception {
//                return ((value.f3 > 0)&& (value.f4> 0)&&(value.f0.trim().equals("SSE.600056")));
//            }
//        }).print();
        /*
         * 计算KD指标
         * 使用买卖一档平均价作为收盘价
         * 使用基于时间窗口的处理方式
         * 返回结果为字符串，直接写入kafka的topic：“KD-zx”
         * */
        DataStream<String> result = streamTuple.keyBy(0).timeWindow(windowTime,slideTime).allowedLateness(Latency)
                .apply(new KDTimeWindow()).startNewChain().setParallelism(5);//.addSink(produce);
        //result.print();
        result.addSink(produce).setParallelism(4);
        /*
         * 计算KD指标
         * 使用买卖一档平均价作为收盘价
         * 使用基于行情笔数窗口的处理方式
         * 返回结果为字符串，直接写入kafka的topic：“KD-zx”
         * */
//        streamTuple.keyBy(0).countWindow(windowSize)
//                .apply(new KDCountWindow()).print();

        /*
         * 计算KD指标
         * 使用买卖一档平均价作为收盘价
         * 使用基于时间窗口的处理方式
         * 返回结果为字符串，直接写入kafka的topic：“KD-zx”
         * */
//        streamTuple.keyBy(0).timeWindow(windowTime,slideTime).allowedLateness(Latency)
//                .apply(new WindowFunction<Tuple8<String,String, String,Double, Double, Double, Double,Long>, String,
//                        Tuple, TimeWindow>() {
//                    private transient ValueState<Tuple3<String, Double,Double>> sum;
//
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<String, String, String, Double, Double, Double, Double, Long>> iterable, Collector<String>  collector)
//                            throws Exception {
//                        Double MaxPrice = 0.0;
//                        Double MinPrice = 0.0;
//                        Double K ,D,RSV,closePrice = 0.0;
//                        for(Tuple8<String, String, String, Double, Double, Double, Double, Long> record:iterable){
//                            if(record.f3 >= MaxPrice ) {
//                                MaxPrice = record.f3;
//                            }
//                            if(record.f3 <= MinPrice){
//                                MinPrice = record.f3;
//                            }
//                            closePrice = (record.f4+record.f5 - 2*MinPrice)/2.0;
//                        }
//                        RSV = (closePrice - MinPrice)/(MaxPrice - MinPrice)*100;
//                        K = (2/3.0)*50 + (1/3.0)*RSV;
//                        D = (2/3.0)*50 + (1/3.0)*K;
//                        Tuple8<String, String, String, Double, Double, Double, Double, Long> result=  iterable.iterator().next();
//                        Date date =new Date(timeWindow.getEnd());
//                        SimpleDateFormat formatter=new SimpleDateFormat("HHmmss");
//                        String time = formatter.format(date);
//                        //  System.out.println(time);
//                        // Tuple3<String, Double,Double> currentSum = sum.value();
//                        // currentSum.f0 = result.f1;
//                        // currentSum.f1 = K;
//                        //currentSum.f2 = D;
//                        //System.out.println(currentSum.f0);
//                        //System.out.println(currentSum.f1);
//                        //System.out.println(currentSum.f2);
//                        collector.collect(result.f1+","+result.f0+","+time+","+K.toString()+","+D.toString());
//                    }
//                }).print(); //.addSink(produce);
        /*
         * 计算KD指标
         * 使用买卖一档平均价作为收盘价
         * 使用基于时间窗口的处理方式
         * 返回结果为tuple5，直接打印到控制台
         * */
//        streamTuple.keyBy(0).timeWindow(windowTime,slideTime).allowedLateness(Latency)
//                .apply(new WindowFunction<Tuple8<String,String, String,Double, Double, Double, Double,Long>, Tuple5<String,String,String,Double,Double>,
//                        Tuple, TimeWindow>() {
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<String, String, String, Double, Double, Double, Double, Long>> iterable, Collector<Tuple5<String,String,String,Double,Double>>  collector)
//                            throws Exception {
//                        Double MaxPrice = 0.0;
//                        Double MinPrice = 0.0;
//                        Double K ,D,RSV,closePrice = 0.0;
//                        for(Tuple8<String, String, String, Double, Double, Double, Double, Long> record:iterable){
//                            if(record.f3 >= MaxPrice ) {
//                                MaxPrice = record.f3;
//                            }
//                            if(record.f3 <= MinPrice){
//                                MinPrice = record.f3;
//                            }
//                            closePrice = (record.f4+record.f5 - 2*MinPrice)/2.0;
//                        }
//                        RSV = (closePrice - MinPrice)/(MaxPrice - MinPrice)*100;
//                        K = (2/3.0)*50 + (1/3.0)*RSV;
//                        D = (2/3.0)*50 + (1/3.0)*K;
//                        Tuple8<String, String, String, Double, Double, Double, Double, Long> result=  iterable.iterator().next();
//                        Date date =new Date(timeWindow.getEnd());
//                        SimpleDateFormat formatter=new SimpleDateFormat("HHmmss");
//                        String time = formatter.format(date);
//                        //  System.out.println(time);
//                        collector.collect(new Tuple5<String,String,String,Double,Double>(result.f1,result.f0,time,K,D));
//                    }
//                }).print();
        /*
         * 计算KD指标
         * 使用买卖一档平均价作为收盘价
         * 使用基于行情记录数窗口的处理方式
         * */
//        streamTuple.keyBy(0).countWindow(windowSize).apply(new WindowFunction<Tuple8<String,String, String,Double, Double, Double, Double,Long>, Tuple6<String,String,String,Double,Double,Long>,
//                Tuple,GlobalWindow>(){
//            @Override
//            public void apply(Tuple tuple,GlobalWindow Window, Iterable<Tuple8<String, String, String, Double, Double, Double, Double, Long>> iterable,
//                              Collector<Tuple6<String, String, String, Double, Double, Long>> collector) throws Exception {
//                int count= 0;
//                Double MaxPrice = 0.0;
//                Double MinPrice = 0.0;
//                Double K = 0.0;
//                Double D = 0.0;
//                Double RSV = 0.0;
//                for(Tuple8<String, String, String, Double, Double, Double, Double, Long> record:iterable){
//                    if(record.f3 >= MaxPrice ) {
//                        MaxPrice = record.f3;
//                    }
//                    if(record.f3 <= MinPrice){
//                        MinPrice = record.f3;
//                    }
//                    count++;
//                    if(count == windowSize){
//                        RSV = (record.f4+record.f5 - 2*MinPrice)/(MaxPrice - MinPrice)*50;
//                        K = (2/3.0)*50 + (1/3.0)*RSV;
//                        D = (2/3.0)*50 + (1/3.0)*K;
//                    }
//                }
//                System.out.println(count);
//                Tuple8<String, String, String, Double, Double, Double, Double, Long> result=  iterable.iterator().next();
//                //if(result.f0.trim().equals("SSE.600056")){
//                collector.collect(new Tuple6<String, String,String, Double, Double,Long>(result.f0,result.f1,result.f2,K,D,0L));
//                // }
//            }
//        }).print();

//        streamTuple.keyBy(0).timeWindow(windowTime,slideTime)
//                .apply(new WindowFunction<Tuple8<String,String, String,Double, Double, Double, Double,Long>, Tuple6<String,String,String,Double,Double,Date>,
//                               Tuple, TimeWindow>() {
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<String, String, String, Double, Double, Double, Double, Long>> iterable, Collector<Tuple6<String,String,String,Double,Double,Date>>  collector)
//                            throws Exception {
//                        double sum = 0L;
//                        int count= 0;
//                        for(Tuple8<String, String, String, Double, Double, Double, Double, Long> record:iterable){
//                            sum += record.f6;
//                            count++;
//                        }
//                        Tuple8<String, String, String, Double, Double, Double, Double, Long> result=  iterable.iterator().next();
//
//                       // result.f6= sum;
//                        collector.collect(new Tuple6<String, String,String, Double, Double, Date>(result.f0,result.f1,result.f2,result.f3,result.f4,new Date(timeWindow.getEnd())));
//                       // System.out.println(sum);
//                       // System.out.println(count);
//                    }
//                }).print();
        /*
         * keyby中选择器示例 ，过滤器示例
         * */

//        streamTuple.keyBy(new KeySelector<Tuple8<String,String, String,Double, Double, Double, Double,Long>, String>() {
//            @Override
//            public String getKey(Tuple8<String, String, String, Double, Double, Double, Double, Long> tuple8) throws Exception {
//                return tuple8.f0;
//            }
//        }).countWindow(windowSize).max("f2").filter(new FilterFunction<Tuple8<String,String, String,Double, Double, Double, Double,Long>>() {
//            @Override
//            public boolean filter(Tuple8<String,String, String,Double, Double, Double, Double,Long> value) throws Exception {
//                // return ((value.f0.trim().equals("SSE.600056")));
//                return ((value.f3 > 0)&& (value.f4> 0)&&(value.f0.trim().equals("SSE.600056")));
//            }
//        }).print();
        /*
         * flatMap示例
         * */
//        streamTuple.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String value, Collector<String> out)
//                    throws Exception {
//                for(String line: value.split(" ")){
//                    for(String segment:line.split(",")) {
//                        out.collect(segment);mvn
//                    }
//                }
//            }
//        }).keyBy(1).timeWindowAll(Time.seconds(10),Time.seconds(1)).sum(9).print();
//    }

        /*
         * 数据入hbase示例
         * */
 //       streamTuple.map(new HbaseSink());
//        stream.addSink(produce);//过滤后的行情发送给kafka
        try {
                env.execute("processKD");
                //System.out.println(env.getExecutionPlan());
            } catch (Exception ex) {
              //  Logger.getLogger(processKD.class.getName()).log(Level.SEVERE, null, ex);
                ex.printStackTrace();
            }
        };
}
