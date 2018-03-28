/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.hadoop.io.IntWritable;

import javax.xml.soap.Text;
import java.util.Properties;

public class WriteToKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.130.141:6667");

        DataStream<String> stream = env.addSource(new SimpleStringGenerator());
        FlinkKafkaProducer010<String> produce = new FlinkKafkaProducer010<String>("zx-kafka", new SimpleStringSchema(), properties);
       // produce.setWriteTimestampToKafka(true);
        //BucketingSink hdfs = new BucketingSink<String>("hdfs://gpu1:8020//user/zhangxiao/");
        //BucketingSink hdfs = new BucketingSink<String>("/user/zhangxiao/");
        //hdfs.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd--HHmm"));
        //hdfs.setWriter(new SequenceFileWriter<IntWritable, Text>());
        //hdfs.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
        stream.addSink(produce);
        //stream.addSink(hdfs);
        env.execute();
    }
    /**
     * Simple Class to generate data
     */
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