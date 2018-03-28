import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import java.util.Properties;

public class Producer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.130.141:6667");

        DataStream<String> stream = env.addSource(new SimpleStringGenerator());


        FlinkKafkaProducer010.FlinkKafkaProducer010Configuration myProducerConfig = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
                stream,                     // input stream
                "zx-flink-demo",                 // target topic
                new SimpleStringSchema(),   // serialization schema
                properties);                // custom configuration for KafkaProducer (including broker list)
        myProducerConfig.setWriteTimestampToKafka(true);
        env.execute( );
    }

    public static class SimpleStringGenerator implements SourceFunction<String> {
        private static final long serialVersionUID = 119007289730474249L;
        boolean running = true;
        long i = 0;
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while(running) {
                ctx.collect("FLINK-"+ (i++));
                Thread.sleep(10);
            }
        }
        @Override
        public void cancel() {
            running = false;
        }
    }
}
