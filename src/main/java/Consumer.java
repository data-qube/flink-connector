import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Properties;

public class Consumer {

    public static void main(String[] args) throws Exception {
        // create execution environment

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.130.141:6667");
        properties.setProperty("group.id", "flink_consumer");
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("hqtestzx", new org.apache.flink.streaming.util.serialization.SimpleStringSchema(), properties);
        myConsumer.setStartFromEarliest();
        //myConsumer.setStartFromLatest();
        DataStream<String> stream = env.addSource(myConsumer).setParallelism(1);
        stream.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String value) throws Exception {
                return "KD Index: " + value;
            }
        }).print();
        env.execute("Consumer");
    }
}
