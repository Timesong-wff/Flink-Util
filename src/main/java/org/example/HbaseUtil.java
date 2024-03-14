package org.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @author wang ff
 * @date 2023/9/25 18:01
 * <p>
 * Table类 :dml操作增删改查
 * Admin类 :ddl创建库,删除库
 */
public class HbaseUtil {
    //获取连接
    public static Connection getHbaseConnection() {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }
    
    //关闭连接
    public static void closeHbaseConnection(Connection conn) {
        if (conn != null && !conn.isClosed()) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    //todo 新建表
    
    /**
     * 在hbase中新建表
     * 连接
     * 表空间
     * 表名
     * 列族
     */
    public static void createHbaseTable(Connection connection, String namespace, String tableName, String... families) {
        //自动资源的释放
        try (Admin admin = connection.getAdmin()) {
            if (families.length < 1) {
                throw new IllegalArgumentException("在建表的时候至少需要传递一个列族");
            }
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            //表名表空间判断
            if (admin.tableExists(tableNameObj)) {
                System.out.println("要创建的" + namespace + ":" + tableName + "已存在");
                return;
            }
            //描述器,进行创建表的信息构建
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            
            //将列族创建出来
            for (String family : families) {
                
                ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(build);
            }
            
            //建表
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    //todo 删除表
    public static void dropHbaseTable(Connection connection, String nameSpace, String tableName) {
        try (Admin admin = connection.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            if (!admin.tableExists(tableNameObj)) {
                System.out.println("要删除的" + nameSpace + ":" + tableName + "不存在");
                return;
            }
            
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /** 从Hbase中删除一条数据
     *
     * @param conn          连接
     * @param Namespace     表空间
     * @param tableName     表名
     * @param rowKey        rowkey
     * */
    public static void delRow(Connection conn, String Namespace, String tableName, String rowKey) throws IOException {
        //要操作的信息
        TableName tableNameObj = TableName.valueOf(Namespace, tableName);
        //获取Table对象
        Table table = conn.getTable(tableNameObj);
        //根据RowKey删除
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
        
    }
    
    /**
     * 向Hbase put数据
     *
     * @param conn           连接
     * @param hbaseNamespace 表空间
     * @param tableName      表名
     * @param rowkey         rowkey
     * @param Family         列族
     * @param columnNames    列
     * @param columnValues   列值
     */
    public static void putRow(Connection conn, String hbaseNamespace, String tableName, String rowkey, String Family, String[] columnNames, String[] columnValues) throws IOException {
        TableName tableNameObj = TableName.valueOf(hbaseNamespace, tableName);
        Table table = conn.getTable(tableNameObj);
        //为指定行进行put操作
        Put put = new Put(Bytes.toBytes(rowkey));
        //根据维度表列的多少进行循环 ,
        for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            String columnValue = columnValues[i];
            if (StringUtils.isNotEmpty(columnValue)) {
                put.addColumn(Bytes.toBytes(Family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
            }
        }
        table.put(put);
        table.close();
    }
    
    //获取字典维度表建表语句
    public static String getBaseDicDDL() {
        return "CREATE TABLE base_dic (" +
                // 原子类型会自动识别为 hbase 的 RowKey,只能有一个原子类型. 列名随意
                " dic_code string," +
                // 声明列族,类型必须是 Row 类型, 嵌套的字段名是 hbase 中的列.
                " info ROW<dic_name string>," +
                " PRIMARY KEY (dic_code) NOT ENFORCED" +
                ")" + getHbaseDDL("dim_base_dic");
    }
    
    //获取Hbase连接器相关参数
    public static String getHbaseDDL(String tableName) {
        return " with(" +
                " 'connector' = 'hbase-2.2'," +
                " 'zookeeper.quorum' = 'hadoop102,hadoop103,hadoop104:2181', " +
                " 'table-name' = '" + "namespace" + ":" + tableName + "'," +
                //是否启用异步查找。如果为真，查找将是异步的。
                " 'lookup.async' = 'true', " +
                //维表的缓存策略。 目前支持 NONE（不缓存）和 PARTIAL（只在外部数据库中查找数据时缓存）。
                " 'lookup.cache' = 'PARTIAL'," +
                //查找缓存的最大行数，超过这个值，最旧的行将过期。
                //使用该配置时 "lookup.cache" 必须设置为 "PARTIAL”。
                " 'lookup.partial-cache.max-rows' = '200'," +
                //在记录写入缓存后该记录的最大保留时间。
                " 'lookup.partial-cache.expire-after-write' = '1 hour', " +
                //在缓存中的记录被访问后该记录的最大保留时间。
                " 'lookup.partial-cache.expire-after-access' = '1 hour'" +
                ")";
    }
    
    //根据rowkey从Hbase中查询数据
    
    public static JSONObject getDimInfoFromHbase(Connection conn, String namespace, String tableName, String rowKey){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        //检索用于访问表的表实现。返回的表不是线程安全的，应为每个使用线程创建一个新实例。
        try (Table table = conn.getTable(tableNameObj)){
            
            Get get = new Get(Bytes.toBytes(rowKey));
            //通过查询获取一条数据 , 调用get方法
            Result result = table.get(get);
            
            //获取一列一列的查询 -----> Hbase真正存储的时候是kv类型
            List<Cell> cells = result.listCells();
            //将数据从cell中提取出来
            if (cells != null && !cells.isEmpty()) {
                JSONObject jsonObject = new JSONObject();
                for (Cell cell : cells) {
                    
                    //获取列名列值需要通过以下的方法获取
                    //不用cell.getXXX一类
                    //它的所有的方法获取的对象都是一样的 , 列族 列名 值封装到byte数组里
                    String colName = Bytes.toString(CellUtil.cloneQualifier(cell));//获取列名
                    String colValue = Bytes.toString(CellUtil.cloneValue(cell));//获取列值
                    jsonObject.put(colName,colValue);
                }
                return jsonObject;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //内部返回值了 , 这里返回null
        return null;
    }
    
    
    //获取支持异步操作Hbase的链接
    public static AsyncConnection getAsyncConnection(){
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            //通过链接工厂进行获取连接对象
            return ConnectionFactory.createAsyncConnection(configuration).get();
        }catch (Exception e){
            throw new RuntimeException();
        }
    }
    
    //关闭异步操作Hbase的连接客户端
    public static void closeAsyncConnection(AsyncConnection asyncConnection){
        if(asyncConnection != null && !asyncConnection.isClosed()){
            try {
                asyncConnection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * 异步方式获取表的数据进行操作
     *
     */
    public static JSONObject getDimInfoFromHbaseByAsync(AsyncConnection asyncConnection,String nameSpace,String tableName,String rowKey) {
        try {
            //根据表空间 , 表名获取异步链接getTable需要的参数
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            //获取表
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConnection.getTable(tableNameObj);
            
            //一行数据
            Get get = new Get(Bytes.toBytes(rowKey));
            //第一次get是封装的Funture的结果 , 需要再调用一次get
            
            Result result = asyncTable.get(get).get();
            //每一列进行操作
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() >0) {
                JSONObject jsonObject = new JSONObject();
                for (Cell cell : cells) {
                    String colName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String colValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObject.put(colName,colValue);
                }
                return jsonObject;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return null;
    }
    
    public static void main(String[] args) {
        System.out.println(getDimInfoFromHbase(getHbaseConnection(), "namespace", "dim_base_trademark", "1"));
    }
}
