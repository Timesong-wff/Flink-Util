import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.example.HbaseUtil;
import org.example.HiveConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author WangFF
 * @version 1.0
 * @data 2024/3/14 14:48
 */
public class test {
    public static void main(String[] args) {
        try(Connection connection = HiveConnection.getConnection("gmall","atguigu","1")) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM ads_coupon_stats");

            while (resultSet.next()){
                String dt = resultSet.getString(1);
                String coupon_id = resultSet.getString(2);
                String coupon_name = resultSet.getString(3);
                String use_count = resultSet.getString(4);
                String used_user_count = resultSet.getString(5);
                System.out.println(dt + " " + coupon_id + " " + coupon_name + " " + use_count + " " + used_user_count);
            }
            resultSet.close();
            statement.close();
            HiveConnection.closeConnection(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
