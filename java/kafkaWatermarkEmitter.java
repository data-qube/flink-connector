import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @Author ZhangXiao
 * @Date 2018/3/16  16:43
 **/
public class kafkaWatermarkEmitter implements AssignerWithPunctuatedWatermarks<String> {

//    @Nullable
//    @Override
//    public Watermark checkAndGetNextWatermark(Long lastElement, long extractedTimestamp) {
//        return new Watermark(extractedTimestamp);
//    }
//
//    @Override
//    public long extractTimestamp(Long element, long previousElementTimestamp) {
//        return previousElementTimestamp;
//    }

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {

        System.out.println(extractedTimestamp);
        return new Watermark(extractedTimestamp);
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        return previousElementTimestamp;
    }
}
