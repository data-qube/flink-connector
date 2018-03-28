import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;

public class MySink extends RichSinkFunction<String> {
    private Connection connection = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://gpu2:3306/zhangxiao";
        String username = "zhangxiao";
        String password = "123456";
        Class.forName(driver);
        connection = DriverManager.getConnection(url, username, password);
        String sql = "insert into test(name)values(?);";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        try {
            //4.组装数据，执行插入操作
            ps.setString(1, value);
            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}
