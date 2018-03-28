import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;

import java.io.IOException;

/**
 * @Author ZhangXiao
 * @Date 2018/2/27  16:14
 **/
public class HbaseSink  implements MapFunction<Tuple6<String,String, String,String, String, String> , String> {

        ConnectHbase connect  = null;
        @Override
        public String map(Tuple6<String,String, String,String, String, String> value) throws Exception {
            String result = value.f0+","+value.f1+","+value.f2+","+value.f3+
                    ","+value.f4+","+value.f5;
            if( null == connect  ){
                connect =new ConnectHbase() ;
            }
            System.out.println("enter");
            connect.writeIntoHBase(result);
            System.out.println("leave");
            return result;
        }
}
