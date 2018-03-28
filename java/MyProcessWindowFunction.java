import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class MyProcessWindowFunction extends ProcessWindowFunction<Tuple8<String,String,String,Double,Double,Double,Double,Long>, String, String, TimeWindow> {

    public MyProcessWindowFunction() {
        super();
    }

    public void process(String key, Context context, Iterable<Tuple8<String,String,String,Double,Double,Double,Double,Long>> input, Collector<String> out) {
        long count = 0;
        for (Tuple8<String,String,String,Double,Double,Double,Double,Long> in: input) {
            count++;
        }
        out.collect("Window: " + context.window() + "count: " + count);
    }
}