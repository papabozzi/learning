import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CreateHbaseFormTest {

    public static void main(String[] args) throws IOException {
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("Muxiaoyue:student");
        String MY_NAMESPACE_NAME = "Muxiaoyue";
        String colFamily1 = "info";
        String colFamily2 = "score";
        String[] rowKeys = new String[]{"Tom", "Jerry", "Jack","Rose","Muxiaoyue"};
        String[] studentIds = new String[]{"20210000000001","20210000000002","20210000000003","20210000000004","G20221148010148"};
        String[] classes = new String[] {"1","1","2","2","3"};
        String[] understandings = new String[]{"75", "85", "80", "60", "90"};
        String[] programmings = new String[] {"82","67","80","61","90"};

        //namespace
        NamespaceDescriptor[] list = admin.listNamespaceDescriptors();
        boolean exists = false;
        for (NamespaceDescriptor nsd : list) {
            if (nsd.getName().equals(MY_NAMESPACE_NAME)) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            System.out.println("Creating Namespace [" + MY_NAMESPACE_NAME + "].");
            admin.createNamespace(NamespaceDescriptor
                    .create(MY_NAMESPACE_NAME).build());
        }

        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(colFamily1);
            HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(colFamily2);

            hTableDescriptor.addFamily(hColumnDescriptor1);
            hTableDescriptor.addFamily(hColumnDescriptor2);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        // 插入数据
        for (int i = 0; i < rowKeys.length; i++) {
            Put put = new Put(Bytes.toBytes(rowKeys[i])); // row key
            put.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes(studentIds[i])); // info:student_id
            put.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes(classes[i])); // info:class
            put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes(understandings[i])); // score:understanding
            put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes(programmings[i])); // score:programming
            conn.getTable(tableName).put(put);
            System.out.println("insert " + rowKeys[i]);
        }
        Scan s = new Scan();
        ResultScanner scanner = conn.getTable(tableName).getScanner(s);
        for (Result result = scanner.next(); (result != null); result = scanner.next()) {
            for(Cell cell : result.rawCells()) {
                String r = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(r + "\t" + colName + "\t" + value);
            }
        }



//        // 删除数据
//        Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
//        conn.getTable(tableName).delete(delete);
//        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }
}
