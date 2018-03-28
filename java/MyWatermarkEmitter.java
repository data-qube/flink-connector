import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/* 抽取时间戳，后做添加watermark的操作，
 * 在使用event time的过程中，必须得提供获取时间戳的方案，保证产生watermarks
 */
public class MyWatermarkEmitter implements  AssignerWithPunctuatedWatermarks<String> {
    private static final long serialVersionUID = 1L;
    private static final long delaytime= 0L;
    @Override
    public long extractTimestamp(String arg0, long arg1) {
        if (null != arg0 && arg0.contains(",")) {
            String parts[] = arg0.split(",");
            SimpleDateFormat formatter=new SimpleDateFormat("yyyyMMddHHmmss");
            long result = 0;
            try{
                Date date=formatter.parse(parts[1]);
                TimeStamp ts = new TimeStamp(date);
                result=ts.getTime();
            }catch (ParseException e){
                e.printStackTrace();
            }
            //System.out.println(result);
            return result;
        }
        return 0;
    }
    @Override
    public Watermark checkAndGetNextWatermark(String arg0, long arg1) {
        if (null != arg0 && arg0.contains(",")) {
            String parts[] = arg0.split(",");
            SimpleDateFormat formatter=new SimpleDateFormat("yyyyMMddHHmmss");
            long result = 0;
            try{
                Date date=formatter.parse(parts[1]);
                TimeStamp ts = new TimeStamp(date);
                result=ts.getTime();
            }catch (ParseException e){
                e.printStackTrace();
            }
            //System.out.println(result);
            return new Watermark(result);
        }
        return null;
    }

}