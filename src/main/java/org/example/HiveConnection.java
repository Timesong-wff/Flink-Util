package org.example;

import org.apache.commons.dbcp2.BasicDataSource;
import java.sql.*;

/**
 * @author WangFF
 * @version 1.0
 * @data 2024/3/14 15:07
 */
public class HiveConnection {

    private static BasicDataSource dataSource;

    public static Connection getConnection(String dateBase, String username, String password) throws SQLException {
        if (dataSource == null) {
            synchronized (HiveConnection.class){
                if (dataSource == null) {
                    dataSource = new BasicDataSource();
                    // 设置Hive JDBC驱动类名
                    dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
                    // 设置Hive服务器URL、用户名和密码
                    dataSource.setUrl("jdbc:hive2://hadoop102:10000/"+dateBase);
                    dataSource.setUsername(username);
                    dataSource.setPassword(password);
                    // 连接池参数配置（根据需要调整）
                    dataSource.setMaxTotal(20);
                    dataSource.setMinIdle(5);
                    dataSource.setMaxWaitMillis(30000); // 等待获取连接的最大时间
                }
            }
        }
        return dataSource.getConnection();
    }

    // 可选地提供关闭方法，在应用结束时清理连接池
    public static void closeConnection(Connection connection) throws SQLException {
        if (connection !=null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (SQLException e) {
                // 记录错误信息
                e.printStackTrace();
            }
        }
    }

}
