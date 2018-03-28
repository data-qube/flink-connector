import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class DataTable {
    public static  void  main(String[] args) throws Exception{
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
//        //StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
//        final String kafkaTopic = "topic";
//
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "192.168.130.141:6667");
//
//        DataStream<String> stream = env
//                .addSource(new FlinkKafkaConsumer010<String>("topic", new org.apache.flink.streaming.util.serialization.SimpleStringSchema(), properties));
//        DataStream<Tuple14<String,String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>>
//                streamTuple14 = stream.flatMap(new FlatMapFunction<String, Tuple14<String,String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple14<String,String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>> out)
//                    throws Exception {
//                for(String line: value.split(" ")){
//
//                    Tuple14<String,String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> cols =
//                            new Tuple14<String,String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>();
//                    String[]  colscontext = line.split(",");
//                    //for(String segment:line.split(",")) {
//                    // System.out.println(line.split(",").length);
//                    //}
//                    cols.f0=colscontext[0];
//                    cols.f1=colscontext[1];
//                    cols.f2=Double.valueOf(colscontext[2]);
//                    cols.f3= Double.valueOf(colscontext[3]);
//                    cols.f4= Double.valueOf(colscontext[4]);
//                    cols.f5= Double.valueOf(colscontext[5]);
//                    cols.f6= Double.valueOf(colscontext[6]);
//                    cols.f7= Double.valueOf(colscontext[7]);
//                    cols.f8= Double.valueOf(colscontext[8]);
//                    cols.f9= Double.valueOf(colscontext[9]);
//                    cols.f10= Double.valueOf(colscontext[10]);
//                    cols.f11= Double.valueOf(colscontext[11]);
//                    cols.f12= Double.valueOf(colscontext[12]);
//                    cols.f13= Double.valueOf(colscontext[13]);
//                    out.collect(cols);
//                }
//            }
//        });
////        tableEnv.registerDataStream("depthmarket",streamTuple14,
////                "f0 as symbol,f1 as timestamp,f2 as  preclose,f3 as open,f4 as high,f5 as low," +
////                        "f6 as last,f7 as vol," +
////                        "f8 as turnover,f9 as tickdeals,f10 as bidprice1,f11 as bidvolume1,f12 as askprice1,f13 as askvol1");
//        tableEnv.registerDataStream("depthmarket",streamTuple14,
//                "symbol,timestamp,preclose,open,high,low," +
//                        "last,vol,turnover,tickdeals,bidprice1,bidvolume1,askprice1,askvol1");
////        tableEnv.registerDataStream("depthmarket",streamTuple14);
//       // tableEnv.registerDataStream("myTable", stream);
//        //tableEnv.fromDataStream(stream);
//        Table f0 = tableEnv.scan("depthmarket").select("symbol");
//        String explanation =tableEnv.explain(f0);
//       // Table result= tableEnv.toRetractStream("f0",String);
//        System.out.println(explanation);
//       // f0.printSchema();
////        Table table = tableEnv.sql("select symbol,timestamp,preclose,open from depthmarket");
////        table.printSchema();
////        table.groupBy("f0").select("f0").where("f0='SSE.6000000'");
//       // tableEnv.
//        //Table tableSQL = tableEnv.toTable()
////        System.out.println(table.tableName());
//        //stream.print();
//        //System.out.print(tableEnv.getNormRuleSet());
//        stream.print();
//        env.execute( );
    }
}

