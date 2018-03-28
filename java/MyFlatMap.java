import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Author ZhangXiao
 * @Date 2018/2/27  16:42
 **/
public class MyFlatMap implements FlatMapFunction<String,String> {
    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        for (String line : s.split(" ")) {
            for (String segment : line.split(",")) {
                collector.collect(segment);
            }

        }
    }
}