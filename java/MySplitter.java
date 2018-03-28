import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author ZhangXiao
 * @Date 2018/2/27  15:43
 **/
public class MySplitter implements FlatMapFunction<String, Tuple8<String,String, String,Double, Double, Double, Double,Long>> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(String value, Collector<Tuple8<String,String, String,Double, Double, Double, Double,Long>> out) throws Exception {

        if (null != value && value.contains(",")) {
            String colscontext[] = value.split(",");
            Tuple8<String,String,String, Double, Double, Double, Double,Long> cols = new Tuple8<String,String, String,Double, Double, Double, Double,Long>();
            cols.f0= colscontext[0];                  //security id
            cols.f1= colscontext[1].substring(0,8);   //tradingdate
            cols.f2= colscontext[1].substring(8);     //trading second
            cols.f3= Double.valueOf(colscontext[6]);  //lastprice
            cols.f4= Double.valueOf(colscontext[10]); //bidprice1
            cols.f5= Double.valueOf(colscontext[30]); //askprice1
            cols.f6=(Double.valueOf(colscontext[11])+ Double.valueOf(colscontext[13]))/2.0;
            SimpleDateFormat formatter=new SimpleDateFormat("yyyyMMddHHmmss");
            Date date=formatter.parse(colscontext[1]);
            TimeStamp ts = new TimeStamp(date);
            cols.f7=ts.getTime();
            if(cols.f2.compareTo("093000") >= 0  & cols.f2.compareTo("150000") <= 0){
                out.collect(cols);
            }
        }
    }
}