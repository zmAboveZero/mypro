import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Test2 {
    static Configuration config = null;
    static Connection connection = null;
    static Table table = null;

    public static void main(String[] args) throws IOException {
        config = HBaseConfiguration.create();// 配置
        connection = ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf("mytest2"));
        Scan scan = new Scan();
        System.out.println("查询到的所有一级部门如下：");
        SingleColumnValueFilter scv = new SingleColumnValueFilter("cf".getBytes(), "score".getBytes(), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(100));
        FilterList fl = new FilterList();
        fl.addFilter(scv);
        scan.setFilter(fl);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (KeyValue kv : result.raw()) {
                System.out.print(new String(kv.getRow()) + ":");
                System.out.print(new String(kv.getFamily()) + ":");
                System.out.print(new String(kv.getQualifier()) + " = ");
                System.out.println(toInt(kv.getValue()));
            }
            System.out.println("================");
        }

//        Put put = new Put("006".getBytes());
//        put.addColumn("cf".getBytes(), "score".getBytes(), Bytes.toBytes(320));
//        put.addColumn("cf".getBytes(), "age".getBytes(), "15".getBytes());
//        table.put(put);
    }

    public static int toInt(byte[] bRefArr) {
        int i = 0;
        i += ((bRefArr[0] & 0xff) << 24);
        i += ((bRefArr[1] & 0xff) << 16);
        i += ((bRefArr[2] & 0xff) << 8);
        i += ((bRefArr[3] & 0xff));
        return i;
    }
}
