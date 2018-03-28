import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class ConnectHbase {
    private static TableName tableName = TableName.valueOf("zx");
    private static final String columnFamily = "cf";
    // 与HBase数据库的连接对象
    private static Connection connection;
    // 数据库元数据操作对象
    private static Admin admin;

    public ConnectHbase() throws Exception{
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.addResource("resources/hbase-site.xml");
        config.addResource("resources/core-site.xml");
        config.addResource("resources/hdfs-site.xml");
        config.set("hbase.zookeeper.quorum", "192.168.130.141,192.168.130.142,192.168.130.143");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.setInt("hbase.rpc.timeout", 20000);
        config.setInt("hbase.client.operation.timeout", 30000);
        config.setInt("hbase.client.scanner.timeout.period", 200000);
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
    }

    public  void writeIntoHBase(String m)throws IOException {

        if (!admin.tableExists(tableName)) {
            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(columnFamily)));
        }
        Table t = connection.getTable(tableName);
        TimeStamp ts = new TimeStamp(new Date());
        Date date = ts.getDate();
        int x = 1+(int)(Math.random()*100);
        String key= String.valueOf(x)+"-"+String.valueOf(ts.getTime());
        Put put = new Put(org.apache.hadoop.hbase.util.Bytes.toBytes(key));
      //  Put put = new Put(org.apache.hadoop.hbase.util.Bytes.toBytes(date.toString()));

        put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes(columnFamily), org.apache.hadoop.hbase.util.Bytes.toBytes("zx"),
                org.apache.hadoop.hbase.util.Bytes.toBytes(m));
        t.put(put);
    }

    public  void getFromHBase( )throws IOException {

        Table t = connection.getTable(TableName.valueOf("zx"));
        ResultScanner scanner = t.getScanner(new Scan());
        // 循环输出表中的数据
        for (Result result : scanner) {
            byte[] row = result.getRow();
            System.out.println("row key is:" + new String(row));

            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {

                byte[] familyArray = cell.getFamilyArray();
                byte[] qualifierArray = cell.getQualifierArray();
                byte[] valueArray = cell.getValueArray();

                System.out.println("row value is:" + new String(familyArray) + new String(qualifierArray)
                        + new String(valueArray));
            }
        }
    }

    public  void truncateTable() throws IOException{

        // 取得目标数据表的表名对象
        TableName tableName = TableName.valueOf("zx");
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(tableName)) {
            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(columnFamily)));
        }
        // 设置表状态为无效
        admin.disableTable(tableName);
        // 清空指定表的数据
        admin.truncateTable(tableName, true);
        //设置表状态为有效
        admin.enableTable(tableName);
    }
}
