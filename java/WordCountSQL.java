import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;


public class WordCountSQL {

    public static void main(String[] args) throws Exception {

        // set up execution environment
//        ExecutionEnvironment envSet = ExecutionEnvironment.getExecutionEnvironment();
//       // StreamExecutionEnvironment envStream= StreamExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tEnvSet = TableEnvironment.getTableEnvironment(envSet);
//
//
//        DataSet<WC> input = envSet.fromElements(
//                new WC("Hello", 1),
//                new WC("Hello", 1),
//                new WC("zhangxiao", 3) );
//        tEnvSet.registerDataSet("WordCount", input, "word, frequency");
//        //实现SQL的例子
//        Table tableSQL = tEnvSet.sqlQuery(
//                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");
//        DataSet<WC> ResultSQL = tEnvSet.toDataSet(tableSQL, WC.class);
//        ResultSQL.print();
//
//        //实例table api的例子
//        Table tableStream = tEnvSet.fromDataSet(input);
//        Table filtered = tableStream
//                .groupBy("word")
//                .select("word, frequency.sum as frequency")
//                .filter("frequency = 3");
//        DataSet<WC> result =  tEnvSet.toDataSet(filtered,WC.class) ;//toRetractStream(filtered,WC.class);
//
//        filtered.printSchema();
//        result.print();
    }

    public static class WC {
        public String word;
        public long frequency;

        public WC() {}
        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }
        @Override
        public String toString() {
            return "WC " + word + " " + frequency;
        }
    }
}
