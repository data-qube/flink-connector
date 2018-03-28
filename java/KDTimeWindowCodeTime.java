import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author ZhangXiao
 * @Date 2018/2/27  13:43
 **/
public class KDTimeWindowCodeTime extends RichWindowFunction<Tuple8<String,String, String,Double, Double, Double, Double,Long>, String,
        Tuple, TimeWindow> {
    private transient ValueState<Tuple3<String, Double,Double>> sum;
    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<String, String, String, Double, Double, Double, Double, Long>> iterable, Collector<String> collector) throws Exception {
        Double MaxPrice = 0.0;
        Double MinPrice = 0.0;
        Double K ,D,RSV,closePrice = 0.0;
        Tuple3<String, Double,Double> currentSum = sum.value();
        for(Tuple8<String, String, String, Double, Double, Double, Double, Long> record:iterable){
            if(record.f3 >= MaxPrice ) {
                MaxPrice = record.f3;
            }
            if(record.f3 <= MinPrice){
                MinPrice = record.f3;
            }
            closePrice = record.f3;
        }
        RSV = (closePrice - MinPrice)/(MaxPrice - MinPrice)*100;
        K = (2/3.0)*currentSum.f1 + (1/3.0)*RSV;
        D = (2/3.0)*currentSum.f2 + (1/3.0)*K;
        Tuple8<String, String, String, Double, Double, Double, Double, Long> result=  iterable.iterator().next();
        Date date =new Date(timeWindow.getEnd());
        SimpleDateFormat formatter=new SimpleDateFormat("HHmmss");
        String time1 = formatter.format(date);
        String  time2 = String.valueOf(timeWindow.getEnd());
        String ID =  new StringBuffer(result.f0+time2+result.f2).reverse().toString();
        collector.collect(ID+","+result.f1+","+result.f0+","+time1+","+K.toString()+","+D.toString());
        currentSum.f0 = result.f0;
        currentSum.f1 = K;
        currentSum.f2 = D;
        sum.update(currentSum);
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Tuple3<String, Double,Double>> descriptor =
                new ValueStateDescriptor<>(
                        "KD state", // the state name
                        TypeInformation.of(new TypeHint<Tuple3<String, Double,Double>>() {}), // type information
                        Tuple3.of("ID", 50.0,50.0)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }
}
